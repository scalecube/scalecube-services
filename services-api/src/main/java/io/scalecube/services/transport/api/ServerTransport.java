package io.scalecube.services.transport.api;

import io.scalecube.services.methods.ServiceMethodRegistry;
import reactor.core.publisher.Mono;

/** Server service transport interface. */
public interface ServerTransport {

  ServerTransport NO_SERVER_TRANSPORT = new ServerTransport() {};

  /**
   * Returns factual listen server address.
   *
   * @return listen server address
   */
  default Address address() {
    return Address.NO_ADDRESS;
  }

  /**
   * Starts a server transport.
   *
   * @param port listen port (can be {@code 0})
   * @param methodRegistry service method registry
   * @return bound server address
   */
  default Mono<ServerTransport> bind(int port, ServiceMethodRegistry methodRegistry) {
    return Mono.just(this);
  }

  /**
   * Stops server transport.
   *
   * @return srop signal
   */
  default Mono<Void> stop() {
    return Mono.empty();
  }
}
