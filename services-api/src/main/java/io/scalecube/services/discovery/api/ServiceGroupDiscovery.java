package io.scalecube.services.discovery.api;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ServiceGroupDiscovery {

  Flux<ServiceGroupDiscoveryEvent> listen();

  Mono<ServiceGroupDiscovery> start();

  Mono<Void> shutdown();
}
