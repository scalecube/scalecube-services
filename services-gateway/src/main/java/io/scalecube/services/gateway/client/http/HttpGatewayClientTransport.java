package io.scalecube.services.gateway.client.http;

import static io.scalecube.services.gateway.client.ServiceMessageCodec.decodeData;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelOption;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.scalecube.services.Address;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.gateway.client.GatewayClientCodec;
import io.scalecube.services.transport.api.ClientChannel;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.DataCodec;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.NettyOutbound;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClientRequest;
import reactor.netty.http.client.HttpClientResponse;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.SslProvider;

public final class HttpGatewayClientTransport implements ClientChannel, ClientTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpGatewayClientTransport.class);

  private static final String CONTENT_TYPE = "application/json";
  private static final LoopResources LOOP_RESOURCES = LoopResources.create("http-gateway-client");
  private static final HttpGatewayClientCodec CLIENT_CODEC =
      new HttpGatewayClientCodec(DataCodec.getInstance(CONTENT_TYPE));

  private final GatewayClientCodec clientCodec;
  private final LoopResources loopResources;
  private final Address address;
  private final Duration connectTimeout;
  private final String contentType;
  private final boolean followRedirect;
  private final SslProvider sslProvider;
  private final boolean shouldWiretap;
  private final Map<String, String> headers;

  private ConnectionProvider connectionProvider;
  private final AtomicReference<HttpClient> httpClientReference = new AtomicReference<>();

  private HttpGatewayClientTransport(Builder builder) {
    this.clientCodec = builder.clientCodec;
    this.loopResources = builder.loopResources;
    this.address = builder.address;
    this.connectTimeout = builder.connectTimeout;
    this.contentType = builder.contentType;
    this.followRedirect = builder.followRedirect;
    this.sslProvider = builder.sslProvider;
    this.shouldWiretap = builder.shouldWiretap;
    this.headers = builder.headers;
  }

  @Override
  public ClientChannel create(ServiceReference serviceReference) {
    httpClientReference.getAndUpdate(
        oldValue -> {
          if (oldValue != null) {
            return oldValue;
          }

          connectionProvider = ConnectionProvider.create("http-gateway-client");

          HttpClient httpClient =
              HttpClient.create(connectionProvider)
                  .headers(entries -> headers.forEach(entries::add))
                  .headers(entries -> entries.set("Content-Type", contentType))
                  .followRedirect(followRedirect)
                  .wiretap(shouldWiretap)
                  .runOn(loopResources)
                  .host(address.host())
                  .port(address.port())
                  .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) connectTimeout.toMillis())
                  .option(ChannelOption.TCP_NODELAY, true);

          if (sslProvider != null) {
            httpClient = httpClient.secure(sslProvider);
          }

          return httpClient;
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
    LOGGER.debug("Sending request: {}", request);
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

    LOGGER.debug("Received response: {}", message);
    return message;
  }

  private static boolean isError(HttpResponseStatus status) {
    return status.code() >= 400 && status.code() <= 599;
  }

  @Override
  public void close() {
    if (connectionProvider != null) {
      connectionProvider.dispose();
    }
  }

  public static class Builder {

    private GatewayClientCodec clientCodec = CLIENT_CODEC;
    private LoopResources loopResources = LOOP_RESOURCES;
    private Address address;
    private Duration connectTimeout = Duration.ofSeconds(5);
    private String contentType = CONTENT_TYPE;
    private boolean followRedirect;
    private SslProvider sslProvider;
    private boolean shouldWiretap;
    private Map<String, String> headers = new HashMap<>();

    public Builder() {}

    public GatewayClientCodec clientCodec() {
      return clientCodec;
    }

    public Builder clientCodec(GatewayClientCodec clientCodec) {
      this.clientCodec = clientCodec;
      return this;
    }

    public LoopResources loopResources() {
      return loopResources;
    }

    public Builder loopResources(LoopResources loopResources) {
      this.loopResources = loopResources;
      return this;
    }

    public Address address() {
      return address;
    }

    public Builder address(Address address) {
      this.address = address;
      return this;
    }

    public Duration connectTimeout() {
      return connectTimeout;
    }

    public Builder connectTimeout(Duration connectTimeout) {
      this.connectTimeout = connectTimeout;
      return this;
    }

    public String contentType() {
      return contentType;
    }

    public Builder contentType(String contentType) {
      this.contentType = contentType;
      return this;
    }

    public boolean followRedirect() {
      return followRedirect;
    }

    public Builder followRedirect(boolean followRedirect) {
      this.followRedirect = followRedirect;
      return this;
    }

    public SslProvider sslProvider() {
      return sslProvider;
    }

    public Builder sslProvider(SslProvider sslProvider) {
      this.sslProvider = sslProvider;
      return this;
    }

    public boolean shouldWiretap() {
      return shouldWiretap;
    }

    public Builder shouldWiretap(boolean wiretap) {
      this.shouldWiretap = wiretap;
      return this;
    }

    public Map<String, String> headers() {
      return headers;
    }

    public Builder headers(Map<String, String> headers) {
      this.headers = Collections.unmodifiableMap(new HashMap<>(headers));
      return this;
    }

    public HttpGatewayClientTransport build() {
      return new HttpGatewayClientTransport(this);
    }
  }
}
