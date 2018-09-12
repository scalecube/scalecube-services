package io.scalecube.gateway.clientsdk.websocket;

import io.netty.buffer.ByteBuf;
import io.scalecube.gateway.clientsdk.ClientCodec;
import io.scalecube.gateway.clientsdk.ClientMessage;
import io.scalecube.gateway.clientsdk.ClientSettings;
import io.scalecube.gateway.clientsdk.ClientTransport;
import io.scalecube.gateway.clientsdk.ErrorData;
import io.scalecube.gateway.clientsdk.exceptions.ExceptionProcessor;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
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
                options
                    .disablePool()
                    .compression(false)
                    .connectAddress(() -> address)
                    .loopResources(loopResources));
  }

  @Override
  public Mono<ClientMessage> requestResponse(ClientMessage request) {
    return Mono.defer(
        () -> {
          String sid = String.valueOf(sidCounter.incrementAndGet());
          ByteBuf byteBuf = toRequest(request, sid);

          return getOrConnect()
              .flatMap(
                  session ->
                      session
                          .send(byteBuf)
                          .then(receiveResponse(session.receive(), sid))
                          .doOnCancel(() -> handleCancel(sid, session)));
        });
  }

  @Override
  public Flux<ClientMessage> requestStream(ClientMessage request) {
    return Flux.defer(
        () -> {
          String sid = String.valueOf(sidCounter.incrementAndGet());
          ByteBuf byteBuf = toRequest(request, sid);

          return getOrConnect()
              .flatMapMany(
                  session ->
                      session
                          .send(byteBuf)
                          .thenMany(receiveStream(session.receive(), sid))
                          .doOnCancel(() -> handleCancel(sid, session)));
        });
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

                                  WebsocketSession session = new WebsocketSession(in, out);

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
                .header("sid", sid)
                .header("sig", Signal.CANCEL.codeAsString())
                .build());
    return session.send(byteBuf).subscribe();
  }

  private ClientMessage enrichResponse(ClientMessage message) {
    return ClientMessage.from(message)
        .header("client-recv-time", String.valueOf(System.currentTimeMillis()))
        .build();
  }

  private ByteBuf toRequest(ClientMessage message, String sid) {
    return codec.encode(
        ClientMessage.from(message)
            .header("client-send-time", String.valueOf(System.currentTimeMillis()))
            .header("sid", sid)
            .build());
  }

  private Flux<ClientMessage> receiveStream(Flux<ByteBuf> inbound, String sid) {
    return Flux.create(
        sink ->
            receiveForSid(inbound, sid)
                .subscribe(
                    response -> handleResponse(response, sink::next, sink::complete, sink::error)));
  }

  public Mono<ClientMessage> receiveResponse(Flux<ByteBuf> inbound, String sid) {
    return Mono.create(
        sink ->
            receiveForSid(inbound, sid)
                .subscribe(
                    response ->
                        handleResponse(response, sink::success, sink::success, sink::error)));
  }

  private Flux<ClientMessage> receiveForSid(Flux<ByteBuf> inbound, String sid) {
    return inbound
        .publishOn(Schedulers.single(), Integer.MAX_VALUE)
        .map(codec::decode)
        .filter(response -> sid.equals(response.header("sid")))
        .log(">>> SID_RECEIVE", Level.FINE)
        .map(this::enrichResponse);
  }

  private void handleResponse(
      ClientMessage response,
      Consumer<ClientMessage> onNext,
      Runnable onComplete,
      Consumer<Throwable> onError) {
    try {
      Optional<Signal> signalOptional =
          Optional.ofNullable(response.header("sig")).map(Signal::from);

      if (signalOptional.isPresent()) {
        // handle completion signal
        Signal signal = signalOptional.get();
        if (signal == Signal.COMPLETE) {
          onComplete.run();
        }
        if (signal == Signal.ERROR) {
          // decode error data to retrieve real error cause
          ErrorData errorData = codec.decodeData(response, ErrorData.class).data();
          Throwable e =
              ExceptionProcessor.toException(
                  response.qualifier(), errorData.getErrorCode(), errorData.getErrorMessage());
          String sid = response.header("sid");
          LOGGER.error("Received error response: sid={}, error={}", sid, e);
          onError.accept(e);
        }
      } else {
        // handle normal response
        onNext.accept(response);
      }
    } catch (Exception e) {
      onError.accept(e);
    }
  }
}
