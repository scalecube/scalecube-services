package io.scalecube.services.transport.server.api;

import io.scalecube.services.ServiceMessageCodec;

import java.net.InetSocketAddress;
import java.util.Collection;

import reactor.core.publisher.Mono;

public interface ServerTransport {

  Collection<? extends ServiceMessageCodec> availableServiceMessageCodec();
  
  ServerTransport accept(ServerMessageAcceptor acceptor);

  InetSocketAddress bindAwait(int port);

  Mono<Void> stop();
  
}

