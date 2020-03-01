package io.scalecube.spring;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.ServicesProvider;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class SpringServicesProvider implements ServicesProvider {

  @Override
  public Mono<Collection<ServiceInfo>> provide(Microservices microservices) {
    AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
    context.registerBean(ServiceCall.class, microservices::call);
    context.register(Beans.class);
    context.refresh();
    context.start();

    List<ServiceInfo> beans =
        context.getBeansWithAnnotation(Service.class).values().stream()
            .map(bean -> ServiceInfo.fromServiceInstance(bean).build())
            .collect(Collectors.toList());
    return Mono.just(beans);
  }

  @Configuration
  static class Beans {
    @Bean
    public LocalService localServiceBean(ServiceCall serviceCall) {
      return new LocalServiceBean(serviceCall);
    }
  }

  @Service
  public interface SimpleService {

    @ServiceMethod
    Mono<Long> get();
  }

  @Service
  public interface LocalService {

      @ServiceMethod
      Mono<Long>get();
  }

  public static class LocalServiceBean implements LocalService {

    private final SimpleService serviceCall;

    public LocalServiceBean(ServiceCall serviceCall) {
      this.serviceCall = serviceCall.api(SimpleService.class);
      this.serviceCall.get().subscribe(System.out::println);
    }

    public Mono<Long> get() {
      return serviceCall.get().map(n -> n * -1);
    }
  }
}
