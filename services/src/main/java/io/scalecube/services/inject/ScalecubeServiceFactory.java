package io.scalecube.services.inject;

import io.scalecube.services.ExtendedMicroservicesContext;
import io.scalecube.services.Microservices;
import io.scalecube.services.MicroservicesContext;
import io.scalecube.services.ServiceDefinition;
import io.scalecube.services.ServiceFactory;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.ServiceProvider;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import reactor.core.publisher.Mono;

public class ScalecubeServiceFactory implements ServiceFactory {

  private final Function<MicroservicesContext, Collection<ServiceInfo>> serviceFactory;

  // lazy init
  private final AtomicReference<Collection<ServiceInfo>> services = new AtomicReference<>();

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
    ServiceProvider provider = call -> Stream.of(services)
            .map(service -> {
              ServiceInfo.Builder builder;
              if (service instanceof  ServiceInfo) {
                builder = ServiceInfo.from((ServiceInfo) service);
              } else {
                builder = ServiceInfo.fromServiceInstance(service);
              }
              return builder.build();
            })
            .collect(Collectors.toList());
    return new ScalecubeServiceFactory(Collections.singleton(provider));
  }

  private ScalecubeServiceFactory(Collection<ServiceProvider> serviceProviders) {
    this.serviceFactory =
        microservices ->
            serviceProviders.stream()
                .map(serviceProvider -> serviceProvider.provide(microservices.serviceCall()))
                .flatMap(Collection::stream)
                .map(service -> Injector.inject(microservices, service))
                .collect(Collectors.toList());
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
   *
   * @param microservices
   */
  @Override
  public Mono<? extends Collection<ServiceInfo>> initializeServices(
      ExtendedMicroservicesContext microservices) {
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
  public Mono<Void> shutdownServices(ExtendedMicroservicesContext microservices) {
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
