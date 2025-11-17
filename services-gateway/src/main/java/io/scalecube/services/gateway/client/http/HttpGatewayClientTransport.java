package io.scalecube.services.gateway.client.http;

import static io.netty.handler.codec.http.HttpMethod.DELETE;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.HEAD;
import static io.netty.handler.codec.http.HttpMethod.OPTIONS;
import static io.netty.handler.codec.http.HttpMethod.TRACE;
import static io.scalecube.services.gateway.client.ServiceMessageCodec.decodeData;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelOption;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.scalecube.services.Address;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.gateway.client.GatewayClientCodec;
import io.scalecube.services.transport.api.ClientChannel;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.DataCodec;
import java.lang.reflect.Type;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
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

  private static final String CONTENT_TYPE = "application/json";
  private static final HttpGatewayClientCodec CLIENT_CODEC =
      new HttpGatewayClientCodec(DataCodec.getInstance(CONTENT_TYPE));
  private static final int CONNECT_TIMEOUT_MILLIS = (int) Duration.ofSeconds(5).toMillis();
  private static final Set<HttpMethod> BODYLESS_METHODS = Set.of(GET, HEAD, DELETE, OPTIONS, TRACE);

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

  public static Builder builder() {
    return new Builder();
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
  public Mono<ServiceMessage> requestResponse(ServiceMessage message, Type responseType) {
    return Mono.defer(
        () -> {
          final var httpClient = httpClientReference.get();
          final var method = message.headers().getOrDefault("http.method", "POST");
          final var queryParams = headersByPrefix(message.headers(), "http.query");

          return httpClient
              .request(HttpMethod.valueOf(method))
              .uri(applyQueryParams("/" + message.qualifier(), queryParams))
              .send((request, outbound) -> send(message, request, outbound))
              .responseSingle(
                  (clientResponse, mono) ->
                      mono.defaultIfEmpty(Unpooled.EMPTY_BUFFER)
                          .map(ByteBuf::retain)
                          .map(data -> toMessage(clientResponse, data)))
              .map(msg -> decodeData(msg, responseType));
        });
  }

  private Mono<Void> send(
      ServiceMessage message, HttpClientRequest request, NettyOutbound outbound) {
    // Extract custom headers
    final var messageHeaders = message.headers();
    final var httpHeaders = headersByPrefix(messageHeaders, "http.header");

    // Apply HTTP headers first
    httpHeaders.forEach(request::header);

    // Apply remaining message headers (skip http.*)
    messageHeaders.entrySet().stream()
        .filter(e -> !e.getKey().startsWith("http."))
        .forEach(e -> request.header(e.getKey(), e.getValue()));

    if (BODYLESS_METHODS.contains(request.method())) {
      return outbound.then();
    }

    // Send with publisher (defer buffer cleanup to netty)
    return outbound.sendObject(Mono.just(clientCodec.encode(message))).then();
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
    final var builder =
        ServiceMessage.builder()
            .qualifier(httpResponse.uri())
            .data(data != Unpooled.EMPTY_BUFFER ? data : null);

    if (isError(httpResponse.status())) {
      builder.header(ServiceMessage.HEADER_ERROR_TYPE, httpResponse.status().code());
    }

    // Populate HTTP response headers
    httpResponse
        .responseHeaders()
        .forEach(entry -> builder.header(entry.getKey(), entry.getValue()));

    return builder.build();
  }

  private static boolean isError(HttpResponseStatus status) {
    return status.code() >= 400 && status.code() <= 599;
  }

  private static String applyQueryParams(String uri, Map<String, String> queryParams) {
    if (queryParams != null && !queryParams.isEmpty()) {
      final var queryString =
          queryParams.entrySet().stream()
              .map(
                  e -> {
                    final var key = e.getKey();
                    final var value = e.getValue();
                    final var charset = StandardCharsets.UTF_8;
                    return URLEncoder.encode(key, charset)
                        + "="
                        + URLEncoder.encode(value, charset);
                  })
              .collect(Collectors.joining("&"));
      uri += "?" + queryString;
    }
    return uri;
  }

  private static Map<String, String> headersByPrefix(Map<String, String> headers, String prefix) {
    if (headers == null || headers.isEmpty()) {
      return Map.of();
    }
    final var finalPrefix = prefix + ".";
    final var result = new HashMap<String, String>();
    headers.forEach(
        (k, v) -> {
          if (k.startsWith(finalPrefix)) {
            result.put(k.substring(finalPrefix.length()), v);
          }
        });
    return result;
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

    private Builder() {}

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
