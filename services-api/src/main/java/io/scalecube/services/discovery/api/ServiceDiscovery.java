package io.scalecube.services.discovery.api;

import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.transport.api.Address;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ServiceDiscovery {

  ServiceDiscovery NO_SERVICE_DISCOVERY = new ServiceDiscovery() {};

  default Address address() {
    return Address.NO_ADDRESS;
  }

  default ServiceEndpoint endpoint() {
    return null;
  }

  default Flux<ServiceDiscoveryEvent> listen() {
    return Flux.never();
  }

  default Mono<ServiceDiscovery> start() {
    return Mono.just(this);
  }

  default Mono<Void> shutdown() {
    return Mono.empty();
  }
}
