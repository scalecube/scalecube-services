package io.scalecube.gateway.clientsdk.websocket;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.scalecube.gateway.clientsdk.ClientMessage;
import io.scalecube.gateway.clientsdk.codec.ClientMessageCodec;
import java.util.logging.Level;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.http.websocket.WebsocketInbound;
import reactor.ipc.netty.http.websocket.WebsocketOutbound;

final class WebsocketSession {

  private final WebsocketInbound inbound;
  private final WebsocketOutbound outbound;
  private final ClientMessageCodec<ByteBuf> codec;

  WebsocketSession(WebsocketInbound inbound, WebsocketOutbound outbound, ClientMessageCodec codec) {
    this.inbound = inbound;
    this.outbound = outbound;
    this.codec = codec;
  }

  public Flux<WebSocketFrame> receive() {
    return inbound
        .aggregateFrames()
        .receiveFrames()
        .map(WebSocketFrame::retain)
        .log(">> RECEIVE", Level.FINE);
  }

  public Mono<Void> send(Publisher<ClientMessage> publisher) {
    return null;
    /*return outbound
    .sendObject(Flux.from(publisher).map(TextWebSocketFrame::new).log("<< SEND", Level.FINE))
    .then();*/
  }

  public Mono<Void> close() {
    return outbound
        .sendObject(new CloseWebSocketFrame(1000, "close"))
        .then()
        .log("<< CLOSE", Level.FINE);
  }

  public void onClose(Runnable runnable) {
    inbound.context().onClose(runnable);
  }
}
