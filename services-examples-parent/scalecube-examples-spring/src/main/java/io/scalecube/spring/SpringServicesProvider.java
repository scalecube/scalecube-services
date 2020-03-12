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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import org.springframework.util.ClassUtils;
import reactor.core.publisher.Mono;

public class SpringServicesProvider implements ServicesProvider {

  private final Function<Microservices, AnnotationConfigApplicationContext> ctxFactory;

  // lazy init
  private AnnotationConfigApplicationContext ctx;

  /**
   * Create the instance from Spring Context.
   *
   * @param configurations spring configurations.
   */
  public SpringServicesProvider(Class<?>... configurations) {
    this.ctxFactory =
        microservices -> {
          GenericBeanDefinition discoveryBd = new GenericBeanDefinition();
          discoveryBd.setBeanClass(ServiceDiscovery.class);
          discoveryBd.setLazyInit(true);
          discoveryBd.setSynthetic(true);
          discoveryBd.setInstanceSupplier(microservices::discovery);
          discoveryBd.setSource(this);

          GenericBeanDefinition serviceCallBd = new GenericBeanDefinition();
          serviceCallBd.setBeanClass(ServiceCall.class);
          serviceCallBd.setLazyInit(true);
          serviceCallBd.setSynthetic(true);
          serviceCallBd.setInstanceSupplier(microservices::call);
          serviceCallBd.setSource(this);

          AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
          context.registerBeanDefinition("discovery-" + microservices.id(), discoveryBd);
          context.registerBeanDefinition("serviceCall-" + microservices.id(), serviceCallBd);

          Stream.of(configurations).forEach(context::register);
          return context;
        };
  }

  @Override
  public Mono<Collection<ServiceDefinition>> provideServiceDefinitions(
      Microservices microservices) {
    this.ctx = this.ctxFactory.apply(microservices);
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
    return Mono.just(serviceDefinitions);
  }

  @Override
  public Mono<? extends Collection<ServiceInfo>> provideService(Microservices microservices) {
    this.ctx.refresh();
    this.ctx.start();
    List<ServiceInfo> services =
        this.ctx.getBeansWithAnnotation(Service.class).values().stream()
            .map(bean -> ServiceInfo.fromServiceInstance(bean).build())
            .collect(Collectors.toList());
    return Mono.defer(() -> Mono.just(services));
  }

  @Override
  public Mono<Microservices> shutDown(Microservices microservices) {
    return Mono.fromCallable(
        () -> {
          this.ctx.stop();
          this.ctx.close();
          return microservices;
        });
  }
}
