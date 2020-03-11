package io.scalecube.spring;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.ServicesProvider;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.discovery.api.ServiceDiscovery;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;

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
          AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
          ServiceCall serviceCall = microservices.call();
          context.registerBean(ServiceCall.class, () -> serviceCall);
          Stream.of(configurations).forEach(context::register);
          return context;
        };
  }

  @Override
  public Mono<Collection<ServiceInfo>> constructServices(Microservices microservices) {
    this.ctx = this.ctxFactory.apply(microservices);
    List<ServiceInfo> beans =
        this.ctx.getBeansWithAnnotation(Service.class).values().stream()
            .map(bean -> ServiceInfo.fromServiceInstance(bean).build())
            .collect(Collectors.toList());
    return Mono.just(beans);
  }

  @Override
  public Mono<Void> postConstruct(Microservices microservices) {
    Mono<AnnotationConfigApplicationContext> ctxMono =
        Mono.fromCallable(
            () -> {
              this.ctx.registerBean(ServiceDiscovery.class, microservices::discovery);
              this.ctx.refresh();
              this.ctx.start();
              return this.ctx;
            });
    return Mono.defer(() -> ctxMono).then();
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
