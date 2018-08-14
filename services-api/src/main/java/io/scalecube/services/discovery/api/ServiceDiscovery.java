package io.scalecube.services.discovery.api;

import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceLoaderUtil;
import io.scalecube.transport.Address;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ServiceDiscovery {

  Address address();

  ServiceEndpoint endpoint();

  static ServiceDiscovery getDiscovery() {
    ServiceDiscovery discovery = ServiceLoaderUtil.findFirst(ServiceDiscovery.class)
        .orElseThrow(() -> new IllegalStateException("ServiceDiscovery not configured"));

    return discovery;
  }

  Mono<ServiceDiscovery> start(DiscoveryConfig discoveryConfig);

  Mono<Void> shutdown();

  Flux<DiscoveryEvent> listen();

}
