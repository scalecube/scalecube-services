package io.scalecube.services;

import reactor.core.publisher.Mono;

import java.util.Collection;

import static java.util.Objects.requireNonNull;

@Deprecated
public class DeprecatedServiceProviderAdapter implements ServicesProvider {

  private final ServiceProvider delegate;

  public DeprecatedServiceProviderAdapter(ServiceProvider delegate) {
    this.delegate = requireNonNull(delegate);
  }

  @Override
  public Mono<Collection<ServiceInfo>> provide(IMicroservices microservices) {
    return Mono.defer(() -> Mono.fromSupplier(() -> delegate.provide(microservices.call())));
  }
}
