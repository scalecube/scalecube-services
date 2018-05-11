package io.scalecube.gateway.websocket;

import reactor.core.publisher.Mono;

public interface WebSocketAcceptor {

  Mono<Void> onConnect(WebSocketSession session);

  Mono<Void> onDisconnect(WebSocketSession session);
}
