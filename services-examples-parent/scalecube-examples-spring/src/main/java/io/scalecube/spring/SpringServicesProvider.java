package io.scalecube.spring;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.ServicesProvider;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import org.springframework.beans.factory.support.GenericBeanDefinition;
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
    GenericApplicationContext context = new GenericApplicationContext();
    context.registerBean(ServiceCall.class, microservices::call);
    GenericBeanDefinition bd = new GenericBeanDefinition();
    bd.setBeanClass(Beans.class);
    context.registerBeanDefinition("simple", bd);
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
    public LocalService simpleBean(ServiceCall serviceCall) {
      return new SimpleBean(serviceCall);
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

  public static class SimpleBean implements LocalService {

    private final SimpleService serviceCall;

    public SimpleBean(ServiceCall serviceCall) {
      this.serviceCall = serviceCall.api(SimpleService.class);
      System.out.println( this.serviceCall.get().block(Duration.ofSeconds(1)));
    }

    public Mono<Long> get() {
      return serviceCall.get().map(n -> n * -1);
    }
  }
}
