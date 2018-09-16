package io.scalecube.gateway.clientsdk.http;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpHeaders;
import io.scalecube.gateway.clientsdk.ClientCodec;
import io.scalecube.gateway.clientsdk.ClientMessage;
import io.scalecube.gateway.clientsdk.ClientSettings;
import io.scalecube.gateway.clientsdk.ClientTransport;
import io.scalecube.services.api.Qualifier;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.NettyPipeline.SendOptions;
import reactor.ipc.netty.http.client.HttpClient;
import reactor.ipc.netty.http.client.HttpClientResponse;
import reactor.ipc.netty.resources.LoopResources;
import reactor.ipc.netty.resources.PoolResources;

public final class HttpClientTransport implements ClientTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientTransport.class);

  private static final int WRITE_IDLE_TIMEOUT = 6000;
  private static final int READ_IDLE_TIMEOUT = 6000;

  private final ClientCodec<ByteBuf> codec;
  private final HttpClient httpClient;
  private final PoolResources poolResources;

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

    this.poolResources = PoolResources.elastic("http-client-sdk");

    this.httpClient =
        HttpClient.create(
            options ->
                options
                    .loopResources(loopResources)
                    .poolResources(poolResources)
                    .connectAddress(
                        () ->
                            InetSocketAddress.createUnresolved(settings.host(), settings.port())));
  }

  @Override
  public Mono<ClientMessage> requestResponse(ClientMessage request) {
    return Mono.defer(
        () ->
            httpClient
                .post(
                    request.qualifier(),
                    httpRequest -> {
                      LOGGER.debug("Sending request {}", request);

                      return httpRequest
                          .options(SendOptions::flushOnEach)
                          .header("client-send-time", String.valueOf(System.currentTimeMillis()))
                          .failOnClientError(false)
                          .failOnServerError(false)
                          .keepAlive(true)
                          .onWriteIdle(
                              WRITE_IDLE_TIMEOUT,
                              () -> {
                                // no-op
                              })
                          .sendObject(codec.encode(request));
                    })
                .flatMap(
                    httpResponse ->
                        httpResponse
                            .onReadIdle(
                                READ_IDLE_TIMEOUT,
                                () -> {
                                  // no-op
                                })
                            .receive()
                            .aggregate()
                            .map(ByteBuf::retain)
                            .map(
                                content ->
                                    handleResponseContent(
                                        httpResponse, content, request.qualifier()))
                            .map(
                                response ->
                                    enrichResponse(response, httpResponse.responseHeaders()))));
  }

  @Override
  public Flux<ClientMessage> requestStream(ClientMessage request) {
    throw new UnsupportedOperationException("Request stream is not supported by HTTP/1.x");
  }

  @Override
  public Mono<Void> close() {
    return poolResources
        .disposeLater()
        .doOnTerminate(() -> LOGGER.info("Closed http client sdk transport"));
  }

  private ClientMessage handleResponseContent(
      HttpClientResponse httpResponse, ByteBuf content, String requestQualifier) {

    int httpCode = httpResponse.status().code();
    String qualifier = isError(httpCode) ? Qualifier.asError(httpCode) : requestQualifier;
    ClientMessage message = ClientMessage.builder().qualifier(qualifier).data(content).build();

    LOGGER.debug("Received response {}", message);
    return message;
  }

  private boolean isError(int httpCode) {
    return httpCode >= 400 && httpCode <= 599;
  }

  private ClientMessage enrichResponse(ClientMessage message, HttpHeaders responseHeaders) {
    return ClientMessage.from(message)
        .header("client-recv-time", String.valueOf(System.currentTimeMillis()))
        .header("client-send-time", responseHeaders.get("client-send-time"))
        .header("service-recv-time", responseHeaders.get("service-recv-time"))
        .header("gw-recv-from-client-time", responseHeaders.get("gw-recv-from-client-time"))
        .header("gw-recv-from-service-time", responseHeaders.get("gw-recv-from-service-time"))
        .build();
  }
}
