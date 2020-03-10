package io.scalecube.services.inject;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.ServiceProvider;
import io.scalecube.services.ServicesLifeCycleManager;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import reactor.core.publisher.Mono;

public class ServiceLifeCycleManagerAdapter implements ServicesLifeCycleManager {

  // lazy
  private final AtomicReference<ServicesLifeCycleManager> delegate;

  private final BiFunction<Microservices, ServicesLifeCycleManager, ServicesLifeCycleManager>
      factory;

  /**
   * Create {@link ServicesLifeCycleManager} from old {@link ServiceProvider}.
   *
   * @param serviceProvider old service provider.
   */
  public ServiceLifeCycleManagerAdapter(ServiceProvider serviceProvider) {
    this.factory =
        (microservices, provider) ->
            Objects.isNull(provider)
                ? ScaleCubeServicesLifeCycleManager.from(
                    serviceProvider.provide(microservices.call()))
                : provider;
    this.delegate = new AtomicReference<>(EMPTY);
  }

  @Override
  public Mono<? extends Collection<ServiceInfo>> constructServices(Microservices microservices) {
    return delegate
        .updateAndGet(provider -> factory.apply(microservices, provider))
        .constructServices(microservices);
  }

  @Override
  public Mono<Void> postConstruct(Microservices microservices) {
    return delegate.get().postConstruct(microservices);
  }

  @Override
  public Mono<Microservices> shutDown(Microservices microservices) {
    return this.delegate.get().shutDown(microservices);
  }
}
