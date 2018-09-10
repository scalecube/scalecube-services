package io.scalecube.gateway.clientsdk.websocket;

import io.netty.buffer.ByteBuf;
import io.scalecube.gateway.clientsdk.ClientCodec;
import io.scalecube.gateway.clientsdk.ClientMessage;
import io.scalecube.gateway.clientsdk.ClientSettings;
import io.scalecube.gateway.clientsdk.ClientTransport;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.scheduler.Schedulers;
import reactor.ipc.netty.http.client.HttpClient;
import reactor.ipc.netty.resources.LoopResources;

public final class WebsocketClientTransport implements ClientTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(WebsocketClientTransport.class);

  private static final AtomicReferenceFieldUpdater<WebsocketClientTransport, Mono>
      websocketMonoUpdater =
          AtomicReferenceFieldUpdater.newUpdater(
              WebsocketClientTransport.class, Mono.class, "websocketMono");

  private final ClientCodec<ByteBuf> messageCodec;
  private final InetSocketAddress address;
  private final HttpClient httpClient;
  private final AtomicLong sidCounter = new AtomicLong();

  private volatile Mono<?> websocketMono;

  /**
   * Creates instance of websocket client transport.
   *
   * @param settings client settings
   * @param messageCodec client message codec
   * @param loopResources loop resources
   */
  public WebsocketClientTransport(
      ClientSettings settings, ClientCodec<ByteBuf> messageCodec, LoopResources loopResources) {
    this.messageCodec = messageCodec;

    address = InetSocketAddress.createUnresolved(settings.host(), settings.port());

    httpClient =
        HttpClient.create(
            options ->
                options
                    .disablePool()
                    .compression(false)
                    .connectAddress(() -> address)
                    .loopResources(loopResources));
  }

  @Override
  public Mono<ClientMessage> requestResponse(ClientMessage request) {
    return Mono.defer(
        () ->
            getOrConnect()
                .flatMap(
                    session -> {
                      String sid = String.valueOf(sidCounter.incrementAndGet());
                      return session
                          .send(enrichForSend(requestMessage(request, sid)))
                          .then(session.receive(sid).singleOrEmpty())
                          .publishOn(Schedulers.parallel())
                          .map(this::enrichForRecv)
                          .doOnCancel(() -> session.send(cancelMessage(sid)).subscribe());
                    }));
  }

  @Override
  public Flux<ClientMessage> requestStream(ClientMessage request) {
    return Flux.defer(
        () ->
            getOrConnect()
                .flatMapMany(
                    session -> {
                      String sid = String.valueOf(sidCounter.incrementAndGet());
                      return session
                          .send(enrichForSend(requestMessage(request, sid)))
                          .thenMany(session.receive(sid))
                          .publishOn(Schedulers.parallel())
                          .map(this::enrichForRecv)
                          .doOnCancel(() -> session.send(cancelMessage(sid)).subscribe());
                    }));
  }

  @Override
  public Mono<Void> close() {
    return Mono.defer(
        () -> {
          // noinspection unchecked
          Mono<WebsocketSession> curr = websocketMonoUpdater.get(this);
          return curr == null ? Mono.empty() : curr.flatMap(WebsocketSession::close);
        });
  }

  private Mono<WebsocketSession> getOrConnect() {
    // noinspection unchecked
    return Mono.defer(() -> websocketMonoUpdater.updateAndGet(this, this::getOrConnect0));
  }

  private Mono<WebsocketSession> getOrConnect0(Mono<WebsocketSession> prev) {
    if (prev != null) {
      return prev;
    }

    return httpClient
        .ws("/")
        .flatMap(
            response ->
                Mono.create(
                    (MonoSink<WebsocketSession> sink) ->
                        response
                            .receiveWebsocket(
                                (in, out) -> {
                                  LOGGER.info("Connected successfully to {}", address);

                                  WebsocketSession session =
                                      new WebsocketSession(in, out, messageCodec);

                                  sink.success(session);

                                  return session.onClose(
                                      () ->
                                          LOGGER.info(
                                              "Connection to {} has been closed successfully",
                                              address));
                                })
                            .doOnError(sink::error)
                            .subscribe()))
        .doOnError(
            throwable -> {
              LOGGER.warn("Connection to {} is failed, cause: {}", address, throwable);
              websocketMonoUpdater.getAndSet(this, null);
            })
        .cache();
  }

  private ClientMessage enrichForSend(ClientMessage clientMessage) {
    return ClientMessage.from(clientMessage)
        .header("client-send-time", String.valueOf(System.currentTimeMillis()))
        .build();
  }

  private ClientMessage enrichForRecv(ClientMessage message) {
    return ClientMessage.from(message)
        .header("client-recv-time", String.valueOf(System.currentTimeMillis()))
        .build();
  }

  private ClientMessage requestMessage(ClientMessage request, String sid) {
    return ClientMessage.from(request).header("sid", sid).build();
  }

  private ClientMessage cancelMessage(String sid) {
    return ClientMessage.builder()
        .header("sid", sid)
        .header("sig", Signal.CANCEL.codeAsString())
        .build();
  }
}
