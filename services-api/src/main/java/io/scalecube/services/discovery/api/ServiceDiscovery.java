package io.scalecube.services.discovery.api;

import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceLoaderUtil;
import io.scalecube.services.transport.api.Address;
import java.util.ServiceLoader;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ServiceDiscovery {

  Address address();

  ServiceEndpoint endpoint();

  /**
   * Get the discovery. Uses the {@link ServiceLoader#load(Class)} in order to select the service
   *
   * @return a Service Discovery implementation.
   */
  static ServiceDiscovery getDiscovery() {
    return ServiceLoaderUtil.findFirst(ServiceDiscovery.class)
        .orElseThrow(() -> new IllegalStateException("ServiceDiscovery not configured"));
  }

  Mono<ServiceDiscovery> start(ServiceDiscoveryConfig config);

  Mono<Void> shutdown();

  Flux<ServiceDiscoveryEvent> listen();
}
