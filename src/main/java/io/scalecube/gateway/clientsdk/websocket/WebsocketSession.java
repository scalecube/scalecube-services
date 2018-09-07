package io.scalecube.gateway.clientsdk.websocket;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.scalecube.gateway.clientsdk.ClientMessage;
import io.scalecube.gateway.clientsdk.ErrorData;
import io.scalecube.gateway.clientsdk.codec.ClientMessageCodec;
import io.scalecube.gateway.clientsdk.exceptions.ExceptionProcessor;
import java.util.logging.Level;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.NettyPipeline.SendOptions;
import reactor.ipc.netty.http.websocket.WebsocketInbound;
import reactor.ipc.netty.http.websocket.WebsocketOutbound;

final class WebsocketSession {

  private final WebsocketInbound inbound;
  private final WebsocketOutbound outbound;
  private final ClientMessageCodec<ByteBuf> codec;

  private final DirectProcessor<ClientMessage> inboundProcessor = DirectProcessor.create();
  private final FluxSink<ClientMessage> inboundSink = inboundProcessor.sink();

  WebsocketSession(
      WebsocketInbound inbound, WebsocketOutbound outbound, ClientMessageCodec<ByteBuf> codec) {
    this.inbound = inbound;
    this.outbound = outbound;
    this.codec = codec;

    this.inbound
        .aggregateFrames()
        .receive()
        .map(ByteBuf::retain)
        .map(codec::decode)
        .log(">>> RECEIVE", Level.FINE)
        .subscribe(
            response -> {
              String sig = response.header("sig");
              if (sig != null) {
                Signal signal = Signal.from(sig);
                if (signal == Signal.COMPLETE) {
                  inboundSink.complete();
                }
                if (signal == Signal.ERROR) {
                  inboundSink.error(toError(response));
                }
                return;
              }
              inboundSink.next(response);
            },
            inboundSink::error,
            inboundSink::complete);
  }

  public Mono<Void> send(ClientMessage message) {
    return Mono.defer(
        () ->
            outbound
                .options(SendOptions::flushOnEach)
                .sendObject(
                    Mono.just(message)
                        .map(codec::encode)
                        .map(BinaryWebSocketFrame::new)
                        .log("<<< SEND", Level.FINE))
                .then());
  }

  public Flux<ClientMessage> receive(String sid) {
    return inboundProcessor
        .filter(response -> sid.equals(response.header("sid")))
        .log(">>> SID_RECEIVE", Level.FINE);
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

  private Throwable toError(ClientMessage response) {
    ErrorData errorData = codec.decodeData(response, ErrorData.class).data();
    return ExceptionProcessor.toException(
        response.qualifier(), errorData.getErrorCode(), errorData.getErrorMessage());
  }
}
