package io.scalecube.gateway.websocket;

import io.netty.handler.codec.http.websocketx.WebSocketFrame;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiFunction;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.NettyPipeline;
import reactor.ipc.netty.http.server.HttpServer;
import reactor.ipc.netty.http.server.HttpServerRequest;
import reactor.ipc.netty.http.server.HttpServerResponse;
import reactor.ipc.netty.http.websocket.WebsocketInbound;
import reactor.ipc.netty.http.websocket.WebsocketOutbound;

public class WSServerRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(WSServerRunner.class);

  public static void main(String[] args) {
    BiFunction<HttpServerRequest, HttpServerResponse, Publisher<Void>> handler =
        (HttpServerRequest httpRequest, HttpServerResponse httpResponse) -> {
          // noinspection UnnecessaryLocalVariable
          Mono<Void> voidMono =
              httpResponse.sendWebsocket((WebsocketInbound inbound, WebsocketOutbound outbound) -> {

                inbound.context().onClose(() -> LOGGER.info("Byte byte"));

                // return outbound
                // .options(NettyPipeline.SendOptions::flushOnEach)
                // .sendObject(new CloseWebSocketFrame(1000, "Byte byte"))
                // .then();

                Flux<WebSocketFrame> receiveFlux = inbound
                    .aggregateFrames()
                    .receiveFrames()
                    .doOnNext(WebSocketFrame::retain);

                return outbound
                    .options(NettyPipeline.SendOptions::flushOnEach)
                    .sendObject(receiveFlux)
                    .then();
              });

          return voidMono;
        };

    HttpServer.create(builder -> builder
        .port(8080)
        .validateHeaders(true)
        .maxChunkSize(65536)
        .initialBufferSize(8192)).startAndAwait(handler);
  }
}
