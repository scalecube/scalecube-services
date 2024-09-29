package io.scalecube.services.gateway.client.http;

import io.netty.buffer.ByteBuf;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.DefaultErrorMapper;
import io.scalecube.services.exceptions.ServiceClientErrorMapper;
import io.scalecube.services.gateway.client.GatewayClientCodec;
import io.scalecube.services.transport.api.ClientChannel;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.DataCodec;
import java.lang.reflect.Type;
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

public class HttpGatewayClientTransport implements ClientChannel, ClientTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpGatewayClientTransport.class);

  private final AtomicReference<HttpClient> httpClientReference = new AtomicReference<>();

  private final GatewayClientCodec clientCodec;
  private final LoopResources loopResources;
  private final String host;
  private final int port;
  private final String contentType;
  private final boolean followRedirect;
  private final SslProvider sslProvider;
  private final ServiceClientErrorMapper errorMapper;
  private final boolean shouldWiretap;
  private final Map<String, String> headers;
  private ConnectionProvider connectionProvider;

  private HttpGatewayClientTransport(Builder builder) {
    this.clientCodec = builder.clientCodec;
    this.loopResources = builder.loopResources;
    this.host = builder.host;
    this.port = builder.port;
    this.contentType = builder.contentType;
    this.followRedirect = builder.followRedirect;
    this.sslProvider = builder.sslProvider;
    this.errorMapper = builder.errorMapper;
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
                  .host(host)
                  .port(port);

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
                      mono.map(ByteBuf::retain).map(data -> toMessage(clientResponse, data)));
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

    LOGGER.debug("Received response: {}", message);
    return message;
  }

  private static boolean isError(int httpCode) {
    return httpCode >= 400 && httpCode <= 599;
  }

  @Override
  public void close() {
    final HttpClient httpClient = httpClientReference.get();
    if (httpClient != null) {
      connectionProvider.dispose();
    }
  }

  public static class Builder {

    private static final String CONTENT_TYPE = "application/json";
    private static final String DEFAULT_HOST = "localhost";
    private static final String DEFAULT_CONTENT_TYPE = "application/json";

    private static final LoopResources LOOP_RESOURCES = LoopResources.create("http-gateway-client");
    public static final HttpGatewayClientCodec CLIENT_CODEC =
        new HttpGatewayClientCodec(DataCodec.getInstance(CONTENT_TYPE));

    private GatewayClientCodec clientCodec = CLIENT_CODEC;
    private LoopResources loopResources = LOOP_RESOURCES;
    private String host = DEFAULT_HOST;
    private int port;
    private String contentType = DEFAULT_CONTENT_TYPE;
    private boolean followRedirect;
    private SslProvider sslProvider;
    private ServiceClientErrorMapper errorMapper = DefaultErrorMapper.INSTANCE;
    private boolean shouldWiretap;
    private Map<String, String> headers;

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

    public String host() {
      return host;
    }

    public Builder host(String host) {
      this.host = host;
      return this;
    }

    public int port() {
      return port;
    }

    public Builder port(int port) {
      this.port = port;
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

    public ServiceClientErrorMapper errorMapper() {
      return errorMapper;
    }

    public Builder errorMapper(ServiceClientErrorMapper errorMapper) {
      this.errorMapper = errorMapper;
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
