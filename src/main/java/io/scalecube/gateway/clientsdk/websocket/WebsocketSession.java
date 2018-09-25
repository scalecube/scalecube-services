package io.scalecube.gateway.clientsdk.websocket;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.scalecube.gateway.clientsdk.ClientCodec;
import io.scalecube.gateway.clientsdk.ClientMessage;
import io.scalecube.gateway.clientsdk.ErrorData;
import io.scalecube.gateway.clientsdk.ReferenceCountUtil;
import io.scalecube.gateway.clientsdk.exceptions.ExceptionProcessor;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.logging.Level;
import org.jctools.maps.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;
import reactor.ipc.netty.NettyPipeline.SendOptions;
import reactor.ipc.netty.http.websocket.WebsocketInbound;
import reactor.ipc.netty.http.websocket.WebsocketOutbound;

final class WebsocketSession {

  private static final Logger LOGGER = LoggerFactory.getLogger(WebsocketSession.class);

  private static final String STREAM_ID = "sid";
  private static final String SIGNAL = "sig";

  private final ClientCodec<ByteBuf> codec;
  private final WebsocketInbound inbound;
  private final WebsocketOutbound outbound;

  private final ConcurrentMap<String, UnicastProcessor<ClientMessage>> inboundProcessors =
      new NonBlockingHashMap<>();

  WebsocketSession(
      ClientCodec<ByteBuf> codec, WebsocketInbound inbound, WebsocketOutbound outbound) {
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
            response -> {
              String sid = response.header(STREAM_ID);
              // ignore msgs w/o sid
              if (sid == null) {
                LOGGER.error("Ignore response: {} with null sid", response);
                Optional.ofNullable(response.data()).ifPresent(ReferenceCountUtil::safestRelease);
                return;
              }
              // processor?
              UnicastProcessor<ClientMessage> processor = inboundProcessors.get(sid);
              if (processor == null) {
                LOGGER.error("Can't find processor by sid={} for response: {}", sid, response);
                Optional.ofNullable(response.data()).ifPresent(ReferenceCountUtil::safestRelease);
                return;
              }
              // handle response msg
              handleResponse(
                  response, processor::onNext, processor::onError, processor::onComplete);
            });
  }

  public Mono<Void> send(ByteBuf byteBuf, String sid) {
    return Mono.defer(
        () -> {
          inboundProcessors.computeIfAbsent(sid, key -> UnicastProcessor.create());
          LOGGER.info("Put sid=" + sid);
          return outbound
              .sendObject(Mono.just(new TextWebSocketFrame(byteBuf)).log("<<< SEND", Level.FINE))
              .then();
        });
  }

  public Flux<ClientMessage> receive(String sid) {
    return Flux.defer(
        () -> {
          UnicastProcessor<ClientMessage> processor = inboundProcessors.get(sid);
          if (processor == null) {
            LOGGER.error("Can't find processor by sid={}", sid);
            throw new IllegalStateException("Can't find processor by sid");
          }
          return processor
              .log(">>> SID_RECEIVE")
              .doOnTerminate(
                  () -> {
                    inboundProcessors.remove(sid);
                    LOGGER.info("Removed sid=" + sid);
                  })
              .log(">>> SID_RECEIVE", Level.FINE);
        });
  }

  public Mono<Void> close() {
    return Mono.defer(
        () ->
            outbound
                .sendObject(new CloseWebSocketFrame(1000, "close"))
                .then()
                .log("<<< CLOSE", Level.FINE));
  }

  public Mono<Void> onClose(Runnable runnable) {
    return inbound.context().onClose(runnable).onClose();
  }

  private void handleResponse(
      ClientMessage response,
      Consumer<ClientMessage> onNext,
      Consumer<Throwable> onError,
      Runnable onComplete) {
    String sid = response.header(STREAM_ID);
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
