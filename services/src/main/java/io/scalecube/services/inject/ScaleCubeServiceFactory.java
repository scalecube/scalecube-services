package io.scalecube.services.inject;

import io.scalecube.services.Microservices;
import io.scalecube.services.MicroservicesContext;
import io.scalecube.services.ServiceDefinition;
import io.scalecube.services.ServiceFactory;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.ServiceProvider;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import reactor.core.publisher.Mono;

public class ScaleCubeServiceFactory implements ServiceFactory {

  private final Function<MicroservicesContext, Collection<ServiceInfo>> serviceFactory;

  // lazy init
  private AtomicReference<Collection<ServiceInfo>> services = new AtomicReference<>();

  /**
   * Create the instance from {@link ServiceProvider}.
   *
   * @param serviceProviders old service providers.
   * @return default services provider.
   */
  public static ServiceFactory create(Collection<ServiceProvider> serviceProviders) {
    return new ScaleCubeServiceFactory(serviceProviders);
  }

  private ScaleCubeServiceFactory(Collection<ServiceProvider> serviceProviders) {
    this.serviceFactory =
        microservices ->
            serviceProviders.stream()
                .map(serviceProvider -> serviceProvider.provide(microservices.call()))
                .flatMap(Collection::stream)
                .map(service -> Injector.inject(microservices, service))
                .collect(Collectors.toList());
  }

  /**
   * Since the service instance factory ({@link ServiceProvider}) we have to leave behind does not
   * provide us with information about the types of services produced, there is nothing left for us
   * to do but start creating all the services and then retrieve the type of service, previously
   * saving it as a {@link ScaleCubeServiceFactory} state.
   *
   * <p>{@inheritDoc}
   *
   * <p>Use {@link io.scalecube.services.annotations.Inject} for inject {@link Microservices},
   * {@link io.scalecube.services.ServiceCall}.
   *
   * @see ServiceInfo
   * @see ServiceDefinition
   */
  @Override
  public Mono<? extends Collection<ServiceDefinition>> getServiceDefinitions(
      MicroservicesContext microservices) {
    return Mono.fromCallable(
        () ->
            this.services(microservices).stream()
                .map(
                    serviceInfo ->
                        new ServiceDefinition(
                            serviceInfo.serviceInstance().getClass(), serviceInfo.tags()))
                .collect(Collectors.toList()));
  }

  /**
   * {@inheritDoc}
   *
   * <p>Use {@link io.scalecube.services.annotations.AfterConstruct} for initialization service's
   * instance.
   */
  @Override
  public Mono<? extends Collection<ServiceInfo>> initializeServices(
      MicroservicesContext microservices) {
    return Mono.fromCallable(
        () ->
            this.services(microservices).stream()
                .map(service -> Injector.processAfterConstruct(microservices, service))
                .collect(Collectors.toList()));
  }

  /**
   * {@inheritDoc}
   *
   * <p>Use {@link io.scalecube.services.annotations.BeforeDestroy} for finilization service's
   * instance.
   *
   * @return
   */
  @Override
  public Mono<Void> shutdownServices(MicroservicesContext microservices) {
    return Mono.fromRunnable(() -> shutdown0(microservices));
  }

  private void shutdown0(MicroservicesContext microservices) {
    if (this.services.get() != null) {
      this.services.get().forEach(service -> Injector.processBeforeDestroy(microservices, service));
    }
  }

  private Collection<ServiceInfo> services(MicroservicesContext microservices) {
    return this.services.updateAndGet(
        currentValue ->
            currentValue == null ? this.serviceFactory.apply(microservices) : currentValue);
  }
}
