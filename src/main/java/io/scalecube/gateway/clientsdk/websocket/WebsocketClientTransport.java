package io.scalecube.gateway.clientsdk.websocket;

import io.netty.buffer.ByteBuf;
import io.scalecube.gateway.clientsdk.ClientMessage;
import io.scalecube.gateway.clientsdk.ClientSettings;
import io.scalecube.gateway.clientsdk.ClientTransport;
import io.scalecube.gateway.clientsdk.codec.ClientMessageCodec;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiFunction;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.ipc.netty.http.client.HttpClient;
import reactor.ipc.netty.http.websocket.WebsocketInbound;
import reactor.ipc.netty.http.websocket.WebsocketOutbound;
import reactor.ipc.netty.resources.LoopResources;

public final class WebsocketClientTransport implements ClientTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(WebsocketClientTransport.class);

  private static final AtomicReferenceFieldUpdater<WebsocketClientTransport, Mono>
      websocketMonoUpdater =
          AtomicReferenceFieldUpdater.newUpdater(
              WebsocketClientTransport.class, Mono.class, "websocketMono");

  private final ClientMessageCodec<ByteBuf> messageCodec;
  private final InetSocketAddress address;
  private final HttpClient httpClient;

  private volatile Mono<?> websocketMono;

  /**
   * Creates instance of websocket client transport.
   *
   * @param settings client settings
   * @param messageCodec client message codec
   * @param loopResources loop resources
   */
  public WebsocketClientTransport(
      ClientSettings settings,
      ClientMessageCodec<ByteBuf> messageCodec,
      LoopResources loopResources) {
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

  private final AtomicInteger sidCounter = new AtomicInteger();

  @Override
  public Mono<ClientMessage> requestResponse(ClientMessage request) {
    return requestStream(request).as(Mono::from);
  }

  @Override
  public Flux<ClientMessage> requestStream(ClientMessage request) {
    return Flux.defer(
        () -> {
          String sid = String.valueOf(sidCounter.incrementAndGet());
          return getOrConnect()
              .doOnSuccess(
                  session -> {
                    ClientMessage request1 = ClientMessage.from(request).header("sid", sid).build();
                    session.send(request1);
                  })
              .flatMapMany(
                  session ->
                      session.receive().filter(response -> sid.equals(response.header("sid"))))
              .publishOn(Schedulers.parallel());
        });
  }

  @Override
  public Mono<Void> close() {
    return Mono.defer(
        () -> {
          // noinspection unchecked
          Mono<WebsocketSession> curr = websocketMonoUpdater.get(this);
          if (curr == null) {
            return Mono.empty();
          }
          return curr.flatMap(WebsocketSession::close);
        });
  }

  private Mono<WebsocketSession> getOrConnect() {
    return Mono.defer(
        () -> {
          // noinspection unchecked
          return websocketMonoUpdater.updateAndGet(this, this::getOrConnect0);
        });
  }

  private Mono<WebsocketSession> getOrConnect0(Mono<WebsocketSession> prev) {
    if (prev != null) {
      return prev;
    }

    return httpClient
        .ws("/")
        .flatMap(
            response -> {
              WebsocketHandler handler = new WebsocketHandler();
              return response
                  .receiveWebsocket(handler)
                  .then(Mono.defer(() -> Mono.just(handler.session)));
            })
        .doOnError(
            throwable -> {
              LOGGER.warn("Connection to {} is failed, cause: {}", address, throwable);
              websocketMonoUpdater.getAndSet(this, null);
            })
        .cache();
  }

  private class WebsocketHandler
      implements BiFunction<WebsocketInbound, WebsocketOutbound, Publisher<Void>> {

    private DirectProcessor processor;
    private FluxSink sink;
    private WebsocketSession session;

    @Override
    public Publisher<Void> apply(WebsocketInbound inbound, WebsocketOutbound outbound) {
      LOGGER.info("Connected successfully to {}", address);

      processor = DirectProcessor.create();
      sink = processor.sink();

      session = new WebsocketSession(inbound, outbound, messageCodec);
      session.onClose(
          () -> {
            sink.complete();
            LOGGER.info("Connection to {} has been closed successfully", address);
          });

      return session.receive().then();
    }
  }
}
