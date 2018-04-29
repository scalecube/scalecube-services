package io.scalecube.services.transport.server.api;

import java.net.InetSocketAddress;

import reactor.core.publisher.Mono;

public interface ServerTransport {

  ServerTransport accept(ServerMessageAcceptor acceptor);

  InetSocketAddress bindAwait(InetSocketAddress address);

  Mono<Void> stop();
  
}

