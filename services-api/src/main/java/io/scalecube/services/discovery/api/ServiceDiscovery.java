package io.scalecube.services.discovery.api;

import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.transport.api.Address;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ServiceDiscovery {

  Address address();

  ServiceEndpoint endpoint();

  Flux<ServiceDiscoveryEvent> listen();

  Mono<ServiceDiscovery> start();

  Mono<Void> shutdown();
}
