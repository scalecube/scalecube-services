package io.scalecube.gateway.clientsdk.rsocket;

import io.scalecube.gateway.clientsdk.ClientMessage;
import io.scalecube.gateway.clientsdk.ClientSettings;
import io.scalecube.gateway.clientsdk.ClientTransport;
import io.scalecube.gateway.clientsdk.codec.ClientMessageCodec;
import io.scalecube.gateway.clientsdk.exceptions.ConnectionClosedException;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.ipc.netty.http.client.HttpClient;
import reactor.ipc.netty.resources.LoopResources;

import io.rsocket.Frame;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import io.rsocket.util.ByteBufPayload;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public final class RSocketClientTransport implements ClientTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketClientTransport.class);

  private static final AtomicReferenceFieldUpdater<RSocketClientTransport, Mono> rSocketMonoUpdater =
      AtomicReferenceFieldUpdater.newUpdater(RSocketClientTransport.class, Mono.class, "rSocketMono");

  private final ClientSettings settings;
  private final ClientMessageCodec messageCodec;
  private final WebsocketClientTransport clientTransport;

  @SuppressWarnings("unused")
  private volatile Mono<RSocket> rSocketMono;

  public RSocketClientTransport(ClientSettings settings,
      ClientMessageCodec messageCodec,
      LoopResources loopResources) {

    this.settings = settings;
    this.messageCodec = messageCodec;

    // init rSocket's WebsocketClientTransport

    HttpClient httpClient = HttpClient.create(options -> options.disablePool()
        .connectAddress(() -> InetSocketAddress.createUnresolved(settings.host(), settings.port()))
        .loopResources(loopResources));

    clientTransport = WebsocketClientTransport.create(httpClient, "/");
  }

  @Override
  public Mono<ClientMessage> requestResponse(ClientMessage request) {
    return getOrConnect()
        .flatMap(rSocket -> rSocket.requestResponse(toPayload(request))
            .takeUntilOther(listenConnectionClose(rSocket)))
        .map(this::toClientMessage);
  }

  @Override
  public Flux<ClientMessage> requestStream(ClientMessage request) {
    return getOrConnect()
        .flatMapMany(rSocket -> rSocket.requestStream(toPayload(request))
            .takeUntilOther(listenConnectionClose(rSocket)))
        .map(this::toClientMessage);
  }

  @Override
  public Mono<Void> close() {
    // noinspection unchecked
    Mono<RSocket> curr = rSocketMonoUpdater.get(this);
    if (curr == null) {
      return Mono.empty();
    }

    MonoProcessor<Void> sink = MonoProcessor.create();
    curr.subscribe(rSocket -> {
      rSocket.onClose().subscribe(sink::onNext, sink::onError, sink::onComplete);
      rSocket.dispose();
    });
    return sink;
  }

  private Mono<RSocket> getOrConnect() {
    // noinspection unchecked
    return rSocketMonoUpdater.updateAndGet(this, this::getOrConnect0);
  }

  private Mono<RSocket> getOrConnect0(Mono prev) {
    if (prev != null) {
      // noinspection unchecked
      return prev;
    }
    return RSocketFactory.connect()
        .metadataMimeType(settings.contentType())
        .frameDecoder(this::toPayload)
        .transport(clientTransport)
        .start()
        .doOnSuccess(rSocket -> {
          LOGGER.info("Connected successfully: {}", settings);
          // setup shutdown hook
          rSocket.onClose().doOnTerminate(() -> {
            rSocketMonoUpdater.getAndSet(this, null); // clean reference
            LOGGER.info("Connection closed: {}", settings);
          }).subscribe();
        })
        .doOnError(throwable -> {
          LOGGER.warn("Connect failed: {}, cause: {}", settings, throwable);
          rSocketMonoUpdater.getAndSet(this, null); // clean reference
        })
        .cache();
  }

  private Payload toPayload(Frame frame) {
    return ByteBufPayload.create(frame.sliceData().retain(), frame.sliceMetadata().retain());
  }

  private Payload toPayload(ClientMessage clientMessage) {
    return messageCodec.encodeAndTransform(clientMessage, ByteBufPayload::create);
  }

  private ClientMessage toClientMessage(Payload payload) {
    return messageCodec.decode(payload.sliceData(), payload.sliceMetadata());
  }

  @SuppressWarnings("unchecked")
  private <T> Mono<T> listenConnectionClose(RSocket rSocket) {
    return rSocket.onClose()
        .map(aVoid -> (T) aVoid)
        .switchIfEmpty(Mono.defer(this::toConnectionClosedException));
  }

  private <T> Mono<T> toConnectionClosedException() {
    return Mono.error(new ConnectionClosedException("Connection closed"));
  }
}
