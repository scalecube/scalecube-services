package io.scalecube.services;

import io.scalecube.net.Address;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class NoOpServiceDiscovery implements ServiceDiscovery {

  public static final NoOpServiceDiscovery INSTANCE = new NoOpServiceDiscovery();

  @Override
  public Address address() {
    return null;
  }

  @Override
  public ServiceEndpoint serviceEndpoint() {
    return null;
  }

  @Override
  public Flux<ServiceDiscoveryEvent> listenDiscovery() {
    return Flux.empty();
  }

  @Override
  public Mono<ServiceDiscovery> start() {
    return Mono.empty();
  }

  @Override
  public Mono<Void> shutdown() {
    return Mono.empty();
  }
}
