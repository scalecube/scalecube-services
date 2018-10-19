package io.scalecube.services.transport.api;

import io.scalecube.services.methods.ServiceMethodRegistry;
import java.net.InetSocketAddress;
import reactor.core.publisher.Mono;

/** Server service transport interface. */
public interface ServerTransport {

  /**
   * Starts a server transport.
   *
   * @param port listen port
   * @param methodRegistry service method registry
   * @return bound socket address
   */
  Mono<InetSocketAddress> bind(int port, ServiceMethodRegistry methodRegistry);

  /**
   * Stops server transport.
   *
   * @return srop signal
   */
  Mono<Void> stop();
}
