package io.scalecube.services.transport.api;

import io.scalecube.services.methods.ServiceMethodRegistry;
import java.net.InetSocketAddress;
import reactor.core.publisher.Mono;

/** Server service transport interface. */
public interface ServerTransport {

  /**
   * Starts a server transport.
   *
   * @param address listen address where to bind
   * @param methodRegistry service method registry
   * @return bound socket address
   */
  Mono<InetSocketAddress> bind(InetSocketAddress address, ServiceMethodRegistry methodRegistry);

  /**
   * Stops server transport.
   *
   * @return srop signal
   */
  Mono<Void> stop();
}
