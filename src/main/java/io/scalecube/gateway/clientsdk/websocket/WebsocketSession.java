package io.scalecube.gateway.clientsdk.websocket;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.scalecube.gateway.clientsdk.ClientMessage;
import io.scalecube.gateway.clientsdk.codec.ClientMessageCodec;
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
  private final DirectProcessor<WebSocketFrame> outboundProcessor = DirectProcessor.create();
  private final FluxSink<WebSocketFrame> outboundSink = outboundProcessor.sink();

  WebsocketSession(
      WebsocketInbound inbound, WebsocketOutbound outbound, ClientMessageCodec<ByteBuf> codec) {
    this.inbound = inbound;
    this.outbound = outbound;
    this.codec = codec;

    this.outbound
        .options(SendOptions::flushOnEach)
        .sendObject(outboundProcessor.log("<<< SEND", Level.INFO))
        .then()
        .subscribe(
            System.out::println, System.err::println, () -> System.out.println("$$$ Complete"));
  }

  public Mono<Void> send(ClientMessage message) {
    return Mono.fromRunnable(
        () -> {
          ByteBuf encodedMessage = codec.encode(message);
          BinaryWebSocketFrame frame = new BinaryWebSocketFrame(encodedMessage);
          outboundSink.next(frame);
        });
  }

  public Flux<ClientMessage> receive() {
    return inbound
        .aggregateFrames()
        .receive()
        .map(ByteBuf::retain)
        .map(codec::decode)
        .log(">>> RECEIVE", Level.INFO);
  }

  public Mono<Void> close() {
    return Mono.fromRunnable(
        () -> {
          outboundSink.next(new CloseWebSocketFrame(1000, "close"));
          outboundSink.complete();
        });
  }

  public void onClose(Runnable runnable) {
    inbound.context().onClose(runnable);
  }
}
