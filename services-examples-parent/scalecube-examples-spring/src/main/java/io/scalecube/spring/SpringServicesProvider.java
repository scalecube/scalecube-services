package io.scalecube.spring;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.ServiceDefinition;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.ServicesProvider;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.util.ClassUtils;
import reactor.core.publisher.Mono;

public class SpringServicesProvider implements ServicesProvider {

  private final Class<?>[] configurations;

  // lazy init
  private AnnotationConfigApplicationContext ctx;

  /**
   * Create the instance from Spring Context.
   *
   * @param configurations spring configurations.
   */
  public SpringServicesProvider(Class<?>... configurations) {
    this.configurations = configurations;
  }

  @Override
  public Mono<Collection<ServiceDefinition>> provideServiceDefinitions(
      Microservices microservices) {

    this.ctx = new AnnotationConfigApplicationContext();
    String id = microservices.id();
    this.ctx.registerBeanDefinition(
        "discovery-" + id, createBeanDef(ServiceDiscovery.class, microservices::discovery));
    this.ctx.registerBeanDefinition(
        "serviceCall-" + id, createBeanDef(ServiceCall.class, microservices::call));

    Stream.of(this.configurations).forEach(this.ctx::register);

    String[] serviceBeanNames = this.ctx.getBeanDefinitionNames();
    List<ServiceDefinition> serviceDefinitions =
        Stream.of(serviceBeanNames)
            .map(serviceBeanName -> this.ctx.getBeanDefinition(serviceBeanName))
            .map(BeanDefinition::getBeanClassName)
            .filter(Objects::nonNull)
            .map(className -> ClassUtils.resolveClassName(className, ctx.getClassLoader()))
            .filter(type -> type.getAnnotation(Service.class) != null)
            .map(type -> new ServiceDefinition(type, Collections.emptyMap()))
            .collect(Collectors.toList());
    return Mono.defer(() -> Mono.just(serviceDefinitions));
  }

  private <T> GenericBeanDefinition createBeanDef(Class<T> type, Supplier<T> instanceSupplier) {
    GenericBeanDefinition discoveryBd = new GenericBeanDefinition();
    discoveryBd.setBeanClass(type);
    discoveryBd.setLazyInit(true);
    discoveryBd.setSynthetic(true);
    discoveryBd.setInstanceSupplier(instanceSupplier);
    discoveryBd.setSource(this);
    return discoveryBd;
  }

  @Override
  public Mono<? extends Collection<ServiceInfo>> provideService(Microservices microservices) {
    return Mono.defer(
        () -> {
          this.ctx.refresh();
          this.ctx.start();
          List<ServiceInfo> services =
              this.ctx.getBeansWithAnnotation(Service.class).values().stream()
                  .map(bean -> ServiceInfo.fromServiceInstance(bean).build())
                  .collect(Collectors.toList());
          return Mono.just(services);
        });
  }

  @Override
  public Mono<Microservices> shutDown(Microservices microservices) {
    return Mono.defer(
        () ->
            Mono.fromCallable(
                () -> {
                  this.ctx.stop();
                  this.ctx.close();
                  return microservices;
                }));
  }
}
