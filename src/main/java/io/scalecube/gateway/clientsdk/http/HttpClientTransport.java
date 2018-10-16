package io.scalecube.gateway.clientsdk.http;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpHeaders;
import io.scalecube.gateway.clientsdk.ClientCodec;
import io.scalecube.gateway.clientsdk.ClientMessage;
import io.scalecube.gateway.clientsdk.ClientMessage.Builder;
import io.scalecube.gateway.clientsdk.ClientSettings;
import io.scalecube.gateway.clientsdk.ClientTransport;
import io.scalecube.services.api.Qualifier;
import java.net.InetSocketAddress;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClientResponse;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;

public final class HttpClientTransport implements ClientTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientTransport.class);

  private static final String SERVICE_RECV_TIME = "service-recv-time";
  private static final String SERVICE_SEND_TIME = "service-send-time";
  private static final String CLIENT_RECV_TIME = "client-recv-time";
  private static final String CLIENT_SEND_TIME = "client-send-time";

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
                    tcpClient
                        .runOn(loopResources)
                        .addressSupplier(
                            () ->
                                InetSocketAddress.createUnresolved(
                                    settings.host(), settings.port())));
  }

  @Override
  public Mono<ClientMessage> requestResponse(ClientMessage request, Scheduler scheduler) {
    return Mono.defer(
        () -> {
          ByteBuf byteBuf = codec.encode(request);
          return httpClient
              .post()
              .uri(request.qualifier())
              .send(
                  (httpRequest, out) -> {
                    LOGGER.debug("Sending request {}", request);
                    httpRequest.header(
                        CLIENT_SEND_TIME, String.valueOf(System.currentTimeMillis()));
                    return out.sendObject(byteBuf).then();
                  })
              .responseSingle(
                  (httpResponse, bbMono) ->
                      bbMono
                          .map(ByteBuf::retain)
                          .publishOn(scheduler)
                          .map(
                              content ->
                                  toClientMessage(httpResponse, content, request.qualifier())));
        });
  }

  @Override
  public Flux<ClientMessage> requestStream(ClientMessage request, Scheduler scheduler) {
    return Flux.error(
        new UnsupportedOperationException("Request stream is not supported by HTTP/1.x"));
  }

  @Override
  public Mono<Void> close() {
    return connectionProvider
        .disposeLater()
        .doOnTerminate(() -> LOGGER.info("Closed http-client-sdk transport"));
  }

  private ClientMessage toClientMessage(
      HttpClientResponse httpResponse, ByteBuf content, String requestQualifier) {

    int httpCode = httpResponse.status().code();
    String qualifier = isError(httpCode) ? Qualifier.asError(httpCode) : requestQualifier;
    HttpHeaders responseHeaders = httpResponse.responseHeaders();

    Builder builder =
        ClientMessage.builder()
            .qualifier(qualifier)
            .header(CLIENT_RECV_TIME, System.currentTimeMillis());

    Optional.ofNullable(responseHeaders.get(CLIENT_SEND_TIME))
        .ifPresent(value -> builder.header(CLIENT_SEND_TIME, value));

    Optional.ofNullable(responseHeaders.get(SERVICE_RECV_TIME))
        .ifPresent(value -> builder.header(SERVICE_RECV_TIME, value));

    Optional.ofNullable(responseHeaders.get(SERVICE_SEND_TIME))
        .ifPresent(value -> builder.header(SERVICE_SEND_TIME, value));

    ClientMessage message = builder.data(content).build();

    LOGGER.debug("Received response {}", message);
    return message;
  }

  private boolean isError(int httpCode) {
    return httpCode >= 400 && httpCode <= 599;
  }
}
