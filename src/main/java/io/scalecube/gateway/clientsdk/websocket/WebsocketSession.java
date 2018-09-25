package io.scalecube.gateway.clientsdk.websocket;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.scalecube.gateway.clientsdk.ClientCodec;
import io.scalecube.gateway.clientsdk.ClientMessage;
import io.scalecube.gateway.clientsdk.ErrorData;
import io.scalecube.gateway.clientsdk.ReferenceCountUtil;
import io.scalecube.gateway.clientsdk.exceptions.ExceptionProcessor;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.logging.Level;
import org.jctools.maps.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.ipc.netty.NettyPipeline.SendOptions;
import reactor.ipc.netty.http.websocket.WebsocketInbound;
import reactor.ipc.netty.http.websocket.WebsocketOutbound;

final class WebsocketSession {

  private static final Logger LOGGER = LoggerFactory.getLogger(WebsocketSession.class);

  // close ws session normally status code
  private static final int STATUS_CODE_CLOSE = 1000;

  private static final String STREAM_ID = "sid";
  private static final String SIGNAL = "sig";

  private final String id; // keep id for tracing
  private final ClientCodec<ByteBuf> codec;
  private final WebsocketInbound inbound;
  private final WebsocketOutbound outbound;

  // processor by sid mapping
  private final ConcurrentMap<Long, UnicastProcessor<ClientMessage>> inboundProcessors =
      new NonBlockingHashMap<>();

  WebsocketSession(
      ClientCodec<ByteBuf> codec, WebsocketInbound inbound, WebsocketOutbound outbound) {
    this.id = Integer.toHexString(System.identityHashCode(this));
    this.codec = codec;
    this.inbound = inbound;
    this.outbound = (WebsocketOutbound) outbound.options(SendOptions::flushOnEach);

    this.inbound
        .aggregateFrames()
        .receive()
        .retain()
        .map(codec::decode)
        .log(">>> RECEIVE", Level.FINE)
        .subscribe(
            msg -> {
              // ignore msgs w/o sid
              if (!msg.hasHeader(STREAM_ID)) {
                LOGGER.error("Ignore response: {} with null sid, session={}", msg, id);
                Optional.ofNullable(msg.data()).ifPresent(ReferenceCountUtil::safestRelease);
                return;
              }
              long sid = Long.valueOf(msg.header(STREAM_ID));
              // processor?
              UnicastProcessor<ClientMessage> processor = inboundProcessors.get(sid);
              if (processor == null) {
                LOGGER.error(
                    "Can't find processor by sid={} for response: {}, session={}", sid, msg, id);
                Optional.ofNullable(msg.data()).ifPresent(ReferenceCountUtil::safestRelease);
                return;
              }
              // handle response msg
              handleResponse(msg, processor::onNext, processor::onError, processor::onComplete);
            });
  }

  public Mono<Void> send(ByteBuf byteBuf, long sid) {
    return outbound
        .sendObject(
            Mono.<WebSocketFrame>fromCallable(() -> new TextWebSocketFrame(byteBuf))
                .log("<<< SEND", Level.FINE))
        .then()
        .doOnSuccess(
            avoid -> {
              inboundProcessors.computeIfAbsent(sid, key -> UnicastProcessor.create());
              LOGGER.debug("Put sid={}, session={}", sid, id);
            });
  }

  public Flux<ClientMessage> receive(long sid, Scheduler scheduler) {
    return Flux.defer(
        () -> {
          UnicastProcessor<ClientMessage> processor = inboundProcessors.get(sid);
          if (processor == null) {
            LOGGER.error("Can't find processor by sid={}, session={}", sid, id);
            throw new IllegalStateException("Can't find processor by sid");
          }
          return processor
              .publishOn(scheduler)
              .doOnTerminate(
                  () -> {
                    inboundProcessors.remove(sid);
                    LOGGER.debug("Removed sid={}, session={}", sid, id);
                  });
        });
  }

  public Mono<Void> close() {
    Callable<WebSocketFrame> callable = () -> new CloseWebSocketFrame(STATUS_CODE_CLOSE, "close");
    return outbound.sendObject(Mono.fromCallable(callable).log("<<< CLOSE", Level.FINE)).then();
  }

  public Mono<Void> onClose(Runnable runnable) {
    return inbound.context().onClose(runnable).onClose();
  }

  private void handleResponse(
      ClientMessage response,
      Consumer<ClientMessage> onNext,
      Consumer<Throwable> onError,
      Runnable onComplete) {
    try {
      Optional<Signal> signalOptional =
          Optional.ofNullable(response.header(SIGNAL)).map(Signal::from);

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
          String sid = response.header(STREAM_ID);
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

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("client-sdk.WebsocketSession{");
    sb.append("id='").append(id).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
