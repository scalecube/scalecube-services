package io.scalecube.services;

import static java.util.Objects.requireNonNull;

import java.util.Collection;
import reactor.core.publisher.Mono;

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
