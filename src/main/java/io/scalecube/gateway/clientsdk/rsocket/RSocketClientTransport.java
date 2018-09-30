package io.scalecube.gateway.clientsdk.rsocket;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import io.rsocket.util.ByteBufPayload;
import io.scalecube.gateway.clientsdk.ClientCodec;
import io.scalecube.gateway.clientsdk.ClientMessage;
import io.scalecube.gateway.clientsdk.ClientSettings;
import io.scalecube.gateway.clientsdk.ClientTransport;
import io.scalecube.gateway.clientsdk.exceptions.ConnectionClosedException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.ipc.netty.http.client.HttpClient;
import reactor.ipc.netty.resources.LoopResources;

public final class RSocketClientTransport implements ClientTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketClientTransport.class);

  private static final String CLIENT_RECV_TIME = "client-recv-time";
  private static final String CLIENT_SEND_TIME = "client-send-time";

  private static final AtomicReferenceFieldUpdater<RSocketClientTransport, Mono>
      rSocketMonoUpdater =
          AtomicReferenceFieldUpdater.newUpdater(
              RSocketClientTransport.class, Mono.class, "rsocketMono");

  private final ClientSettings settings;
  private final ClientCodec<Payload> codec;
  private final LoopResources loopResources;

  private volatile Mono<?> rsocketMono;

  /**
   * Constructor for client sdk rsocket transport.
   *
   * @param settings client settings.
   * @param codec client message codec.
   * @param loopResources loop resources.
   */
  public RSocketClientTransport(
      ClientSettings settings, ClientCodec<Payload> codec, LoopResources loopResources) {
    this.settings = settings;
    this.codec = codec;
    this.loopResources = loopResources;
  }

  @Override
  public Mono<ClientMessage> requestResponse(ClientMessage request, Scheduler scheduler) {
    return Mono.defer(
        () -> {
          Payload payload = toPayload(enrichRequest(request));
          return getOrConnect()
              .flatMap(
                  rsocket ->
                      rsocket
                          .requestResponse(payload)
                          .takeUntilOther(listenConnectionClose(rsocket)))
              .publishOn(scheduler)
              .map(this::toClientMessage)
              .map(this::enrichResponse);
        });
  }

  @Override
  public Flux<ClientMessage> requestStream(ClientMessage request, Scheduler scheduler) {
    return Flux.defer(
        () -> {
          Payload payload = toPayload(enrichRequest(request));
          return getOrConnect()
              .flatMapMany(
                  rsocket -> {
                    Flux<Payload> requestStream = rsocket.requestStream(payload);
                    if (request.rateLimit() != null) {
                      requestStream = requestStream.limitRate(request.rateLimit());
                    }
                    return requestStream.takeUntilOther(listenConnectionClose(rsocket));
                  })
              .publishOn(scheduler)
              .map(this::toClientMessage)
              .map(this::enrichResponse);
        });
  }

  @Override
  public Mono<Void> close() {
    return Mono.defer(
        () -> {
          // noinspection unchecked
          Mono<RSocket> curr = rSocketMonoUpdater.get(this);
          return (curr == null ? Mono.<Void>empty() : curr.flatMap(this::dispose))
              .doOnTerminate(() -> LOGGER.info("Closed rsocket client sdk transport"));
        });
  }

  private Mono<? extends Void> dispose(RSocket rsocket) {
    rsocket.dispose();
    return rsocket.onClose();
  }

  private Mono<RSocket> getOrConnect() {
    // noinspection unchecked
    return Mono.defer(() -> rSocketMonoUpdater.updateAndGet(this, this::getOrConnect0));
  }

  private Mono<RSocket> getOrConnect0(Mono prev) {
    if (prev != null) {
      // noinspection unchecked
      return prev;
    }

    InetSocketAddress address =
        InetSocketAddress.createUnresolved(settings.host(), settings.port());

    return RSocketFactory.connect()
        .metadataMimeType(settings.contentType())
        .frameDecoder(
            frame ->
                ByteBufPayload.create(frame.sliceData().retain(), frame.sliceMetadata().retain()))
        .transport(createRSocketTransport(address))
        .start()
        .doOnSuccess(
            rsocket -> {
              LOGGER.info("Connected successfully on {}", address);
              // setup shutdown hook
              rsocket
                  .onClose()
                  .doOnTerminate(
                      () -> {
                        rSocketMonoUpdater.getAndSet(this, null); // clean reference
                        LOGGER.info("Connection closed on {}", address);
                      })
                  .subscribe();
            })
        .doOnError(
            throwable -> {
              LOGGER.warn("Connection failed on {}, cause: {}", address, throwable);
              rSocketMonoUpdater.getAndSet(this, null); // clean reference
            })
        .cache();
  }

  private WebsocketClientTransport createRSocketTransport(InetSocketAddress address) {
    String path = "/";
    return WebsocketClientTransport.create(
        HttpClient.create(
            options ->
                options.disablePool().connectAddress(() -> address).loopResources(loopResources)),
        path);
  }

  private Payload toPayload(ClientMessage clientMessage) {
    return codec.encode(clientMessage);
  }

  private ClientMessage toClientMessage(Payload payload) {
    return codec.decode(payload);
  }

  private ClientMessage enrichRequest(ClientMessage clientMessage) {
    return ClientMessage.from(clientMessage)
        .header(CLIENT_SEND_TIME, System.currentTimeMillis())
        .build();
  }

  private ClientMessage enrichResponse(ClientMessage message) {
    return ClientMessage.from(message).header(CLIENT_RECV_TIME, System.currentTimeMillis()).build();
  }

  private <T> Mono<T> listenConnectionClose(RSocket rsocket) {
    //noinspection unchecked
    return rsocket
        .onClose()
        .map(avoid -> (T) avoid)
        .switchIfEmpty(Mono.defer(this::toConnectionClosedException));
  }

  private <T> Mono<T> toConnectionClosedException() {
    return Mono.error(new ConnectionClosedException("Connection closed"));
  }
}
