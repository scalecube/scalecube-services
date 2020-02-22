package io.scalecube.services;

import static java.util.Objects.requireNonNull;

import java.util.Collection;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import reactor.core.publisher.Mono;

@Deprecated
public class DeprecatedServiceProviderAdapter implements ServicesProvider {

  private final Collection<ServiceProvider> delegates;

  public DeprecatedServiceProviderAdapter(Collection<ServiceProvider> delegates) {
    this.delegates = requireNonNull(delegates);
  }

  @Override
  public Mono<Collection<ServiceInfo>> provide(Microservices microservices) {
    Supplier<Mono<Collection<ServiceInfo>>> beanSupplier =
        () ->
            Mono.just(
                delegates.stream()
                    .map(delegate -> delegate.provide(microservices.call()))
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList()));
    return Mono.defer(beanSupplier);
  }
}
