package io.scalecube.gateway.clientsdk.websocket;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.scalecube.gateway.clientsdk.ClientCodec;
import io.scalecube.gateway.clientsdk.ClientMessage;
import io.scalecube.gateway.clientsdk.ErrorData;
import io.scalecube.gateway.clientsdk.exceptions.ExceptionProcessor;
import java.util.Optional;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.ipc.netty.NettyPipeline.SendOptions;
import reactor.ipc.netty.http.websocket.WebsocketInbound;
import reactor.ipc.netty.http.websocket.WebsocketOutbound;

final class WebsocketSession {

  static final Logger LOGGER = LoggerFactory.getLogger(WebsocketSession.class);

  private final WebsocketInbound inbound;
  private final WebsocketOutbound outbound;
  private final ClientCodec<ByteBuf> codec;

  private final DirectProcessor<ClientMessage> inboundProcessor = DirectProcessor.create();
  private final FluxSink<ClientMessage> inboundSink = inboundProcessor.serialize().sink();

  WebsocketSession(
      WebsocketInbound inbound, WebsocketOutbound outbound, ClientCodec<ByteBuf> codec) {
    this.inbound = inbound;
    this.outbound = outbound;
    this.codec = codec;

    this.inbound
        .aggregateFrames()
        .receive()
        .map(ByteBuf::retain)
        .publishOn(Schedulers.single(), Integer.MAX_VALUE) // offload netty thread
        .map(codec::decode)
        .log(">>> RECEIVE", Level.FINE)
        .subscribe(inboundSink::next, inboundSink::error, inboundSink::complete);
  }

  public Mono<Void> send(WebSocketFrame frame) {
    return outbound
        .options(SendOptions::flushOnEach)
        .sendObject(Mono.just(frame).log("<<< SEND", Level.FINE))
        .then();
  }

  public Flux<ClientMessage> receiveStream(String sid) {
    return Flux.create(
        (FluxSink<ClientMessage> sink) ->
            inboundProcessor
                .filter(response -> sid.equals(response.header("sid")))
                .log(">>> SID_RECEIVE", Level.FINE)
                .subscribe(response -> handleResponse(sink, response)));
  }

  public Mono<ClientMessage> receiveResponse(String sid) {
    return Mono.create(
        sink -> receiveStream(sid).subscribe(sink::success, sink::error, sink::success));
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

  private void handleResponse(FluxSink<ClientMessage> sink, ClientMessage response) {
    try {
      Optional<Signal> signal = Optional.ofNullable(response.header("sig")).map(Signal::from);
      if (signal.isPresent()) {
        handleSignal(sink, response, signal.get());
      } else {
        sink.next(response);
      }
    } catch (Exception e) {
      sink.error(e);
    }
  }

  private void handleSignal(FluxSink<ClientMessage> sink, ClientMessage response, Signal signal) {
    if (signal == Signal.COMPLETE) {
      sink.complete();
    }
    if (signal == Signal.ERROR) {
      Throwable e = toError(response);
      LOGGER.error(
          "Received error response on session {}: sid={}, error={}",
          this,
          response.header("sid"),
          e);
      sink.error(e);
    }
  }

  private Throwable toError(ClientMessage response) {
    ErrorData errorData = codec.decodeData(response, ErrorData.class).data();
    return ExceptionProcessor.toException(
        response.qualifier(), errorData.getErrorCode(), errorData.getErrorMessage());
  }
}
