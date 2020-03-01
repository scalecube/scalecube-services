package io.scalecube.services;

import java.util.Collection;
import reactor.core.publisher.Mono;

@FunctionalInterface
public interface ServicesProvider {

  Mono<? extends Collection<ServiceInfo>> provide(Microservices microservices);
}
