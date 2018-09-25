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
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.scheduler.Scheduler;
import reactor.ipc.netty.http.client.HttpClient;
import reactor.ipc.netty.resources.LoopResources;

public final class WebsocketClientTransport implements ClientTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(WebsocketClientTransport.class);

  private static final String CLIENT_RECV_TIME = "client-recv-time";
  private static final String CLIENT_SEND_TIME = "client-send-time";

  private static final String STREAM_ID = "sid";
  private static final String SIGNAL = "sig";

  private static final AtomicReferenceFieldUpdater<WebsocketClientTransport, Mono>
      websocketMonoUpdater =
          AtomicReferenceFieldUpdater.newUpdater(
              WebsocketClientTransport.class, Mono.class, "websocketMono");

  private final ClientCodec<ByteBuf> codec;
  private final InetSocketAddress address;
  private final HttpClient httpClient;
  private final AtomicLong sidCounter = new AtomicLong();

  private volatile Mono<?> websocketMono;

  /**
   * Creates instance of websocket client transport.
   *
   * @param settings client settings
   * @param codec client message codec
   * @param loopResources loop resources
   */
  public WebsocketClientTransport(
      ClientSettings settings, ClientCodec<ByteBuf> codec, LoopResources loopResources) {
    this.codec = codec;

    address = InetSocketAddress.createUnresolved(settings.host(), settings.port());

    httpClient =
        HttpClient.create(
            options ->
                options.disablePool().connectAddress(() -> address).loopResources(loopResources));
  }

  @Override
  public Mono<ClientMessage> requestResponse(ClientMessage request, Scheduler scheduler) {
    return Mono.defer(
        () -> {
          String sid = String.valueOf(sidCounter.incrementAndGet());
          ByteBuf byteBuf = enrichRequest(request, sid);
          return getOrConnect()
              .flatMap(
                  session ->
                      session
                          .send(byteBuf)
                          .then(
                              Mono.<ClientMessage>create(
                                  sink ->
                                      session
                                          .receive(sid)
                                          .map(this::enrichResponse)
                                          .subscribe(sink::success, sink::error, sink::success)))
                          .doOnCancel(() -> handleCancel(sid, session)));
        });
  }

  @Override
  public Flux<ClientMessage> requestStream(ClientMessage request, Scheduler scheduler) {
    return Flux.defer(
        () -> {
          String sid = String.valueOf(sidCounter.incrementAndGet());
          ByteBuf byteBuf = enrichRequest(request, sid);
          return getOrConnect()
              .flatMapMany(
                  session ->
                      session
                          .send(byteBuf)
                          .thenMany(
                              Flux.<ClientMessage>create(
                                  sink ->
                                      session
                                          .receive(sid)
                                          .map(this::enrichResponse)
                                          .subscribe(sink::next, sink::error, sink::complete)))
                          .doOnCancel(() -> handleCancel(sid, session)));
        });
  }

  @Override
  public Mono<Void> close() {
    return Mono.defer(
        () -> {
          // noinspection unchecked
          Mono<WebsocketSession> curr = websocketMonoUpdater.get(this);
          return (curr == null ? Mono.<Void>empty() : curr.flatMap(WebsocketSession::close))
              .doOnTerminate(() -> LOGGER.info("Closed websocket client sdk transport"));
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

                                  WebsocketSession session = new WebsocketSession(codec, in, out);

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

  private Disposable handleCancel(String sid, WebsocketSession session) {
    ByteBuf byteBuf =
        codec.encode(
            ClientMessage.builder()
                .header(STREAM_ID, sid)
                .header(SIGNAL, Signal.CANCEL.codeAsString())
                .build());
    return session.send(byteBuf).subscribe();
  }

  private ByteBuf enrichRequest(ClientMessage message, String sid) {
    return codec.encode(
        ClientMessage.from(message)
            .header(CLIENT_SEND_TIME, String.valueOf(System.currentTimeMillis()))
            .header(STREAM_ID, sid)
            .build());
  }

  private ClientMessage enrichResponse(ClientMessage message) {
    return ClientMessage.from(message)
        .header(CLIENT_RECV_TIME, String.valueOf(System.currentTimeMillis()))
        .build();
  }
}
