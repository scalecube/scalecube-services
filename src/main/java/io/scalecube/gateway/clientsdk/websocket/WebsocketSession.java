package io.scalecube.gateway.clientsdk.websocket;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
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

  private final DirectProcessor<ByteBuf> inboundProcessor = DirectProcessor.create();
  private final FluxSink<ByteBuf> inboundSink = inboundProcessor.serialize().sink();

  WebsocketSession(WebsocketInbound inbound, WebsocketOutbound outbound) {
    this.inbound = inbound;
    this.outbound = outbound;

    this.inbound
        .aggregateFrames()
        .receive()
        .log(">>> RECEIVE", Level.FINE)
        .subscribe(inboundSink::next, inboundSink::error, inboundSink::complete);
  }

  public Mono<Void> send(ByteBuf byteBuf) {
    return outbound
        .options(SendOptions::flushOnEach)
        .sendObject(Mono.just(new TextWebSocketFrame(byteBuf)).log("<<< SEND", Level.FINE))
        .then();
  }

  public Flux<ByteBuf> receive() {
    return inboundProcessor.map(ByteBuf::retain);
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
}
