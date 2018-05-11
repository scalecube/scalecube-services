package io.scalecube.gateway.websocket;

import io.netty.handler.codec.http.websocketx.WebSocketFrame;

import java.net.InetSocketAddress;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.NettyPipeline;
import reactor.ipc.netty.http.websocket.WebsocketInbound;
import reactor.ipc.netty.http.websocket.WebsocketOutbound;

public class WebSocketServerWhyDoesThisSendingEchoRunner {
  public static void main(String[] args) throws InterruptedException {
    WebSocketServer server = new WebSocketServer(new WebSocketAcceptor() {
      @Override
      public Mono<Void> onConnect(WebSocketSession session) {
        WebsocketInbound inbound = session.getInbound();
        WebsocketOutbound outbound = session.getOutbound();

        Flux<WebSocketFrame> receiveFlux = inbound
            .aggregateFrames()
            .receiveFrames()
            .doOnNext(WebSocketFrame::retain);

        // Flux<ServiceMessage> receiveFlux = session.receive();

        return outbound
            .options(NettyPipeline.SendOptions::flushOnEach)
            /* .sendObject(receiveFlux.map(message -> (ByteBuf) message.data()).map(BinaryWebSocketFrame::new).log()) */
            .sendObject(receiveFlux.log())
            .then();
      }

      @Override
      public Mono<Void> onDisconnect(WebSocketSession session) {
        return Mono.empty();
      }
    });
    server.start(new InetSocketAddress(8080));
    Thread.currentThread().join();
  }
}
