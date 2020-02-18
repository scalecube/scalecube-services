package io.scalecube.services;

import reactor.core.publisher.Mono;

import java.util.Collection;

@FunctionalInterface
public interface ServicesProvider {

    Mono<Collection<ServiceInfo>> provide(IMicroservices microservices);
}
