package io.scalecube.services.gateway.client.http;

import static io.scalecube.services.gateway.client.ServiceMessageCodec.decodeData;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelOption;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.scalecube.services.Address;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.gateway.client.GatewayClientCodec;
import io.scalecube.services.transport.api.ClientChannel;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.DataCodec;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.NettyOutbound;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClientRequest;
import reactor.netty.http.client.HttpClientResponse;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;

public final class HttpGatewayClientTransport implements ClientChannel, ClientTransport {

  private static final Logger LOGGER = System.getLogger(HttpGatewayClientTransport.class.getName());

  private static final String CONTENT_TYPE = "application/json";
  private static final HttpGatewayClientCodec CLIENT_CODEC =
      new HttpGatewayClientCodec(DataCodec.getInstance(CONTENT_TYPE));
  private static final int CONNECT_TIMEOUT_MILLIS = (int) Duration.ofSeconds(5).toMillis();

  private final GatewayClientCodec clientCodec;
  private final LoopResources loopResources;
  private final Function<HttpClient, HttpClient> operator;
  private final boolean ownsLoopResources;

  private final AtomicReference<HttpClient> httpClientReference = new AtomicReference<>();

  private HttpGatewayClientTransport(Builder builder) {
    this.clientCodec = builder.clientCodec;
    this.operator = builder.operator;
    this.loopResources =
        builder.loopResources == null
            ? LoopResources.create("http-gateway-client", 1, true)
            : builder.loopResources;
    this.ownsLoopResources = builder.loopResources == null;
  }

  @Override
  public ClientChannel create(ServiceReference serviceReference) {
    httpClientReference.getAndUpdate(
        oldValue -> {
          if (oldValue != null) {
            return oldValue;
          }

          return operator.apply(
              HttpClient.create(ConnectionProvider.create("http-gateway-client"))
                  .runOn(loopResources)
                  .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT_MILLIS)
                  .option(ChannelOption.TCP_NODELAY, true)
                  .headers(headers -> headers.set(HttpHeaderNames.CONTENT_TYPE, CONTENT_TYPE)));
        });
    return this;
  }

  @Override
  public Mono<ServiceMessage> requestResponse(ServiceMessage request, Type responseType) {
    return Mono.defer(
        () -> {
          final HttpClient httpClient = httpClientReference.get();
          return httpClient
              .post()
              .uri("/" + request.qualifier())
              .send((clientRequest, outbound) -> send(request, clientRequest, outbound))
              .responseSingle(
                  (clientResponse, mono) ->
                      mono.map(ByteBuf::retain).map(data -> toMessage(clientResponse, data)))
              .map(msg -> decodeData(msg, responseType));
        });
  }

  private Mono<Void> send(
      ServiceMessage request, HttpClientRequest clientRequest, NettyOutbound outbound) {
    LOGGER.log(Level.DEBUG, "Sending request: {0}", request);
    // prepare request headers
    request.headers().forEach(clientRequest::header);
    // send with publisher (defer buffer cleanup to netty)
    return outbound.sendObject(Mono.just(clientCodec.encode(request))).then();
  }

  @Override
  public Flux<ServiceMessage> requestStream(ServiceMessage message, Type responseType) {
    return Flux.error(new UnsupportedOperationException("requestStream is not supported"));
  }

  @Override
  public Flux<ServiceMessage> requestChannel(
      Publisher<ServiceMessage> publisher, Type responseType) {
    return Flux.error(new UnsupportedOperationException("requestChannel is not supported"));
  }

  private static ServiceMessage toMessage(HttpClientResponse httpResponse, ByteBuf data) {
    ServiceMessage.Builder builder =
        ServiceMessage.builder().qualifier(httpResponse.uri()).data(data);

    HttpResponseStatus status = httpResponse.status();
    if (isError(status)) {
      builder.header(ServiceMessage.HEADER_ERROR_TYPE, status.code());
    }

    // prepare response headers
    httpResponse
        .responseHeaders()
        .entries()
        .forEach(entry -> builder.header(entry.getKey(), entry.getValue()));
    ServiceMessage message = builder.build();

    LOGGER.log(Level.DEBUG, "Received response: {0}", message);
    return message;
  }

  private static boolean isError(HttpResponseStatus status) {
    return status.code() >= 400 && status.code() <= 599;
  }

  @Override
  public void close() {
    if (ownsLoopResources) {
      loopResources.dispose();
    }
  }

  public static class Builder {

    private GatewayClientCodec clientCodec = CLIENT_CODEC;
    private LoopResources loopResources;
    private Function<HttpClient, HttpClient> operator = client -> client;

    public Builder() {}

    public Builder clientCodec(GatewayClientCodec clientCodec) {
      this.clientCodec = clientCodec;
      return this;
    }

    public Builder loopResources(LoopResources loopResources) {
      this.loopResources = loopResources;
      return this;
    }

    public Builder httpClient(UnaryOperator<HttpClient> operator) {
      this.operator = this.operator.andThen(operator);
      return this;
    }

    public Builder address(Address address) {
      return httpClient(client -> client.host(address.host()).port(address.port()));
    }

    public Builder connectTimeout(Duration connectTimeout) {
      return httpClient(
          client ->
              client.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) connectTimeout.toMillis()));
    }

    public Builder contentType(String contentType) {
      return httpClient(
          client ->
              client.headers(headers -> headers.set(HttpHeaderNames.CONTENT_TYPE, contentType)));
    }

    public Builder headers(Map<String, String> headers) {
      return httpClient(client -> client.headers(entries -> headers.forEach(entries::set)));
    }

    public HttpGatewayClientTransport build() {
      return new HttpGatewayClientTransport(this);
    }
  }
}
