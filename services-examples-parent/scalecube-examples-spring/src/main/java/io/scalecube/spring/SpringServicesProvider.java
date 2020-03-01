package io.scalecube.spring;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.ServicesProvider;
import io.scalecube.services.annotations.Service;
import java.util.stream.Stream;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class SpringServicesProvider implements ServicesProvider {

  private final Class<?>[] configurations;

  public SpringServicesProvider(Class<?>... configurations) {
    this.configurations = configurations;
  }

  @Override
  public Mono<Collection<ServiceInfo>> provide(Microservices microservices) {
    AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
    context.registerBean(ServiceCall.class, microservices::call);
    Stream.of(configurations).forEach(context::register);
    context.refresh();
    context.start();

    List<ServiceInfo> beans =
        context.getBeansWithAnnotation(Service.class).values().stream()
            .map(bean -> ServiceInfo.fromServiceInstance(bean).build())
            .collect(Collectors.toList());
    return Mono.just(beans);
  }


}
