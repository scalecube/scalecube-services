package io.scalecube.services;

import io.scalecube.services.inject.Injector;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import reactor.core.publisher.Mono;

public class ScalecubeServiceFactory implements ServiceFactory {

  private final Supplier<Collection<ServiceInfo>> serviceFactory;

  // lazy init
  private final AtomicReference<Collection<ServiceInfo>> services = new AtomicReference<>();
  private Supplier<ServiceCall> serviceCallSupplier;

  private ScalecubeServiceFactory(Collection<ServiceProvider> serviceProviders) {
    this.serviceFactory =
        () -> {
          final ServiceCall serviceCall = this.serviceCallSupplier.get();
          return serviceProviders.stream()
              .map(serviceProvider -> serviceProvider.provide(serviceCall))
              .flatMap(Collection::stream)
              .collect(Collectors.toList());
        };
  }

  /**
   * Create the instance from {@link ServiceProvider}.
   *
   * @param serviceProviders old service providers.
   * @return default services factory.
   * @deprecated use {@link this#fromInstances(Object...)}
   */
  public static ServiceFactory create(Collection<ServiceProvider> serviceProviders) {
    return new ScalecubeServiceFactory(serviceProviders);
  }

  /**
   * Create the instance {@link ServiceFactory} with pre-installed services.
   *
   * @param services user's services
   * @return service factory
   */
  public static ServiceFactory fromInstances(Object... services) {
    ServiceProvider provider =
        call ->
            Stream.of(services)
                .map(
                    service -> {
                      ServiceInfo.Builder builder;
                      if (service instanceof ServiceInfo) {
                        builder = ServiceInfo.from((ServiceInfo) service);
                      } else {
                        builder = ServiceInfo.fromServiceInstance(service);
                      }
                      return builder.build();
                    })
                .collect(Collectors.toList());
    return new ScalecubeServiceFactory(Collections.singleton(provider));
  }

  /**
   * Since the service instance factory ({@link ServiceProvider}) we have to leave behind does not
   * provide us with information about the types of services produced, there is nothing left for us
   * to do but start creating all the services and then retrieve the type of service, previously
   * saving it as a {@link ScalecubeServiceFactory} state.
   *
   * <p>{@inheritDoc}
   *
   * <p>Use {@link io.scalecube.services.annotations.Inject} for inject {@link Microservices},
   * {@link io.scalecube.services.ServiceCall}.
   *
   * @see ServiceInfo
   * @see ServiceDefinition
   * @return
   */
  @Override
  public Collection<ServiceDefinition> getServiceDefinitions() {
    return this.services().stream()
        .map(
            serviceInfo ->
                new ServiceDefinition(serviceInfo.serviceInstance().getClass(), serviceInfo.tags()))
        .collect(Collectors.toList());
  }

  /**
   * {@inheritDoc}
   *
   * <p>Use {@link io.scalecube.services.annotations.AfterConstruct} for initialization service's
   * instance.
   *
   * @param microservices microservices context
   */
  @Override
  public Mono<? extends Collection<ServiceInfo>> initializeServices(
      MicroservicesContext microservices) {
    return Mono.fromCallable(
        () ->
            this.services().stream()
                .map(service -> Injector.inject(microservices, service))
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

  private Collection<ServiceInfo> services() {
    return this.services.updateAndGet(
        currentValue -> currentValue == null ? this.serviceFactory.get() : currentValue);
  }

  /**
   * Setting serviceCall supplier.
   *
   * @param serviceCallSupplier lazy serviceCall initialization function
   * @return current instance scalecube service factory
   * @deprecated see reason in {@link ServiceProvider}
   */
  @Deprecated
  ScalecubeServiceFactory setServiceCall(Supplier<ServiceCall> serviceCallSupplier) {
    this.serviceCallSupplier = serviceCallSupplier;
    return this;
  }
}
