package io.scalecube.services.gateway.clientsdk.http;

import io.netty.buffer.ByteBuf;
import io.scalecube.services.api.Qualifier;
import io.scalecube.services.gateway.clientsdk.ClientCodec;
import io.scalecube.services.gateway.clientsdk.ClientMessage;
import io.scalecube.services.gateway.clientsdk.ClientMessage.Builder;
import io.scalecube.services.gateway.clientsdk.ClientSettings;
import io.scalecube.services.gateway.clientsdk.ClientTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClientResponse;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;

public final class HttpClientTransport implements ClientTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientTransport.class);

  private final ClientCodec<ByteBuf> codec;
  private final HttpClient httpClient;
  private final ConnectionProvider connectionProvider;

  /**
   * Creates instance of http client transport.
   *
   * @param settings client settings
   * @param codec client message codec
   * @param loopResources loop resources
   */
  public HttpClientTransport(
      ClientSettings settings, ClientCodec<ByteBuf> codec, LoopResources loopResources) {

    this.codec = codec;

    connectionProvider = ConnectionProvider.elastic("http-client-sdk");

    httpClient =
        HttpClient.create(connectionProvider)
            .tcpConfiguration(
                tcpClient ->
                    tcpClient.runOn(loopResources).host(settings.host()).port(settings.port()));
  }

  @Override
  public Mono<ClientMessage> requestResponse(ClientMessage request) {
    return Mono.defer(
        () -> {
          ByteBuf byteBuf = codec.encode(request);
          return httpClient
              .post()
              .uri(request.qualifier())
              .send(
                  (httpRequest, out) -> {
                    LOGGER.debug("Sending request {}", request);
                    // prepare request headers
                    request.headers().forEach(httpRequest::header);
                    return out.sendObject(byteBuf).then();
                  })
              .responseSingle(
                  (httpResponse, bbMono) ->
                      bbMono.map(ByteBuf::retain).map(content -> toMessage(httpResponse, content)));
        });
  }

  @Override
  public Flux<ClientMessage> requestStream(ClientMessage request) {
    return Flux.error(
        new UnsupportedOperationException("Request stream is not supported by HTTP/1.x"));
  }

  @Override
  public Mono<Void> close() {
    return connectionProvider
        .disposeLater()
        .doOnTerminate(() -> LOGGER.info("Closed http-client-sdk transport"));
  }

  private ClientMessage toMessage(HttpClientResponse httpResponse, ByteBuf content) {
    int httpCode = httpResponse.status().code();
    String qualifier = isError(httpCode) ? Qualifier.asError(httpCode) : httpResponse.uri();

    Builder builder = ClientMessage.builder().qualifier(qualifier).data(content);
    // prepare response headers
    httpResponse
        .responseHeaders()
        .entries()
        .forEach(entry -> builder.header(entry.getKey(), entry.getValue()));
    ClientMessage message = builder.build();

    LOGGER.debug("Received response {}", message);
    return message;
  }

  private boolean isError(int httpCode) {
    return httpCode >= 400 && httpCode <= 599;
  }
}
