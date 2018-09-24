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
import reactor.ipc.netty.NettyPipeline.SendOptions;
import reactor.ipc.netty.http.client.HttpClient;
import reactor.ipc.netty.http.client.HttpClientResponse;
import reactor.ipc.netty.resources.LoopResources;
import reactor.ipc.netty.resources.PoolResources;

public final class HttpClientTransport implements ClientTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientTransport.class);

  private static final int WRITE_IDLE_TIMEOUT = 6000;
  private static final int READ_IDLE_TIMEOUT = 6000;

  private static final String SERVICE_RECV_TIME = "service-recv-time";
  private static final String SERVICE_SEND_TIME = "service-send-time";
  private static final String CLIENT_RECV_TIME = "client-recv-time";
  private static final String CLIENT_SEND_TIME = "client-send-time";

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

    InetSocketAddress address =
        InetSocketAddress.createUnresolved(settings.host(), settings.port());

    this.httpClient =
        HttpClient.create(
            options ->
                options
                    .loopResources(loopResources)
                    .poolResources(poolResources)
                    .connectAddress(() -> address));
  }

  @Override
  public Mono<ClientMessage> requestResponse(ClientMessage request, Scheduler scheduler) {
    return Mono.defer(
        () -> {
          ByteBuf byteBuf = codec.encode(request);
          return httpClient
              .post(
                  request.qualifier(),
                  httpRequest -> {
                    LOGGER.debug("Sending request {}", request);

                    return httpRequest
                        .options(SendOptions::flushOnEach)
                        .failOnClientError(false)
                        .failOnServerError(false)
                        .keepAlive(true)
                        .onWriteIdle(
                            WRITE_IDLE_TIMEOUT,
                            () -> {
                              // no-op
                            })
                        .header(CLIENT_SEND_TIME, String.valueOf(System.currentTimeMillis()))
                        .sendObject(byteBuf)
                        .then();
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
    return poolResources
        .disposeLater()
        .doOnTerminate(() -> LOGGER.info("Closed http client sdk transport"));
  }

  private ClientMessage toClientMessage(
      HttpClientResponse httpResponse, ByteBuf content, String requestQualifier) {

    int httpCode = httpResponse.status().code();
    String qualifier = isError(httpCode) ? Qualifier.asError(httpCode) : requestQualifier;
    HttpHeaders responseHeaders = httpResponse.responseHeaders();

    Builder builder =
        ClientMessage.builder()
            .qualifier(qualifier)
            .header(CLIENT_RECV_TIME, String.valueOf(System.currentTimeMillis()));

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
