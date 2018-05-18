package io.scalecube.services.transport.server.api;

import io.scalecube.services.api.ServiceMessageHandler;

import java.net.InetSocketAddress;

import reactor.core.publisher.Mono;

public interface ServerTransport {

  InetSocketAddress bindAwait(InetSocketAddress address, ServiceMessageHandler acceptor);

  Mono<Void> stop();

}
