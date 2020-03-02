package io.scalecube.services.inject;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.ServiceProvider;
import io.scalecube.services.ServicesProvider;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;
import reactor.core.publisher.Mono;

public class ServiceProviderAdapter implements ServicesProvider {

  // lazy
  private final AtomicReference<ServicesProvider> delegate;

  private final BiFunction<Microservices, ServicesProvider, ServicesProvider> factory;

  public ServiceProviderAdapter(ServiceProvider serviceProvider) {
    this.factory =
        (microservices, provider) ->
            Objects.isNull(provider)
                ? ScaleCubeServicesProvider.from(serviceProvider.provide(microservices.call()))
                : provider;
    this.delegate = new AtomicReference<>();
  }

  @Override
  public Mono<? extends Collection<ServiceInfo>> provide(Microservices microservices) {
    return delegate
        .updateAndGet(provider -> factory.apply(microservices, provider))
        .provide(microservices);
  }

  @Override
  public Mono<Microservices> shutDown(Microservices microservices) {
    return this.delegate.get().shutDown(microservices);
  }
}
