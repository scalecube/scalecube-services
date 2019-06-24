package io.scalecube.services.transport.api.experimental;

import io.scalecube.net.Address;
import io.scalecube.services.methods.ServiceMethodRegistry;
import reactor.core.publisher.Mono;

/** Server service transport interface. */
public interface ServerTransport {

  /**
   * Returns factual listen server address.
   *
   * @return listen server address
   */
  Address address();

  /**
   * Starts a server transport.
   *
   * @param address host and port
   * @param methodRegistry service method registry
   * @return bound server address
   */
  Mono<ServerTransport> bind(Address address, ServiceMethodRegistry methodRegistry);

  /**
   * Stops server transport.
   *
   * @return srop signal
   */
  Mono<Void> stop();
}
