package io.scalecube.services.gateway.client.http;

import static reactor.core.publisher.Sinks.EmitFailureHandler.busyLooping;

import io.netty.buffer.ByteBuf;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.api.ServiceMessage.Builder;
import io.scalecube.services.gateway.client.GatewayClient;
import io.scalecube.services.gateway.client.GatewayClientCodec;
import io.scalecube.services.gateway.client.GatewayClientSettings;
import java.time.Duration;
import java.util.function.BiFunction;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.NettyOutbound;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClientRequest;
import reactor.netty.http.client.HttpClientResponse;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;

public final class HttpGatewayClient implements GatewayClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpGatewayClient.class);

  private final GatewayClientCodec<ByteBuf> codec;
  private final HttpClient httpClient;
  private final LoopResources loopResources;
  private final boolean ownsLoopResources;

  private final Sinks.One<Void> close = Sinks.one();
  private final Sinks.One<Void> onClose = Sinks.one();

  /**
   * Constructor.
   *
   * @param settings settings
   * @param codec codec
   */
  public HttpGatewayClient(GatewayClientSettings settings, GatewayClientCodec<ByteBuf> codec) {
    this(settings, codec, LoopResources.create("http-gateway-client"), true);
  }

  /**
   * Constructor.
   *
   * @param settings settings
   * @param codec codec
   * @param loopResources loopResources
   */
  public HttpGatewayClient(
      GatewayClientSettings settings,
      GatewayClientCodec<ByteBuf> codec,
      LoopResources loopResources) {
    this(settings, codec, loopResources, false);
  }

  private HttpGatewayClient(
      GatewayClientSettings settings,
      GatewayClientCodec<ByteBuf> codec,
      LoopResources loopResources,
      boolean ownsLoopResources) {

    this.codec = codec;
    this.loopResources = loopResources;
    this.ownsLoopResources = ownsLoopResources;

    HttpClient httpClient =
        HttpClient.create(ConnectionProvider.create("http-gateway-client"))
            .headers(headers -> settings.headers().forEach(headers::add))
            .followRedirect(settings.followRedirect())
            .wiretap(settings.wiretap())
            .runOn(loopResources)
            .host(settings.host())
            .port(settings.port());

    if (settings.sslProvider() != null) {
      httpClient = httpClient.secure(settings.sslProvider());
    }

    this.httpClient = httpClient;

    // Setup cleanup
    close
        .asMono()
        .then(doClose())
        .doFinally(s -> onClose.emitEmpty(busyLooping(Duration.ofSeconds(3))))
        .doOnTerminate(() -> LOGGER.info("Closed HttpGatewayClient resources"))
        .subscribe(null, ex -> LOGGER.warn("Exception occurred on HttpGatewayClient close: " + ex));
  }

  @Override
  public Mono<ServiceMessage> requestResponse(ServiceMessage request) {
    return Mono.defer(
        () -> {
          BiFunction<HttpClientRequest, NettyOutbound, Publisher<Void>> sender =
              (httpRequest, out) -> {
                LOGGER.debug("Sending request {}", request);
                // prepare request headers
                request.headers().forEach(httpRequest::header);
                // send with publisher (defer buffer cleanup to netty)
                return out.sendObject(Mono.just(codec.encode(request))).then();
              };
          return httpClient
              .post()
              .uri("/" + request.qualifier())
              .send(sender)
              .responseSingle(
                  (httpResponse, bbMono) ->
                      bbMono.map(ByteBuf::retain).map(content -> toMessage(httpResponse, content)));
        });
  }

  @Override
  public Flux<ServiceMessage> requestStream(ServiceMessage request) {
    return Flux.error(
        new UnsupportedOperationException("requestStream is not supported by HTTP/1.x"));
  }

  @Override
  public Flux<ServiceMessage> requestChannel(Flux<ServiceMessage> requests) {
    return Flux.error(
        new UnsupportedOperationException("requestChannel is not supported by HTTP/1.x"));
  }

  @Override
  public void close() {
    close.emitEmpty(busyLooping(Duration.ofSeconds(3)));
  }

  @Override
  public Mono<Void> onClose() {
    return onClose.asMono();
  }

  private Mono<Void> doClose() {
    return ownsLoopResources ? Mono.defer(loopResources::disposeLater) : Mono.empty();
  }

  private ServiceMessage toMessage(HttpClientResponse httpResponse, ByteBuf content) {
    Builder builder = ServiceMessage.builder().qualifier(httpResponse.uri()).data(content);

    int httpCode = httpResponse.status().code();
    if (isError(httpCode)) {
      builder.header(ServiceMessage.HEADER_ERROR_TYPE, String.valueOf(httpCode));
    }

    // prepare response headers
    httpResponse
        .responseHeaders()
        .entries()
        .forEach(entry -> builder.header(entry.getKey(), entry.getValue()));
    ServiceMessage message = builder.build();

    LOGGER.debug("Received response {}", message);
    return message;
  }

  private boolean isError(int httpCode) {
    return httpCode >= 400 && httpCode <= 599;
  }
}
