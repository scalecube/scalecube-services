package io.scalecube.spring;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.ServicesLifeCycleManager;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SpringServicesLifeCycleManager implements ServicesLifeCycleManager {

  private final BiFunction<
          Microservices, AnnotationConfigApplicationContext, AnnotationConfigApplicationContext>
      ctxFactory;

  private final AtomicReference<AnnotationConfigApplicationContext> ctx = new AtomicReference<>();

  public SpringServicesLifeCycleManager(Class<?>... configurations) {
    this.ctxFactory =
        (microservices, ctx) -> {
          if (Objects.nonNull(ctx)) {
            return ctx;
          }
          AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
          context.registerBean(ServiceCall.class, microservices::call);
          Stream.of(configurations).forEach(context::register);
          return context;
        };
  }

  @Override
  public Mono<Collection<ServiceInfo>> constructServices(Microservices microservices) {
    AnnotationConfigApplicationContext context =
        this.ctx.updateAndGet(c -> ctxFactory.apply(microservices, c));
    List<ServiceInfo> beans =
        context.getBeansWithAnnotation(Service.class).values().stream()
            .map(bean -> ServiceInfo.fromServiceInstance(bean).build())
            .collect(Collectors.toList());
    return Mono.just(beans);
  }

  @Override
  public Mono<Void> postConstruct(Microservices microservices) {
    Mono<AnnotationConfigApplicationContext> ctxMono =
        Mono.just(
            this.ctx.updateAndGet(
                ctx -> {
                  ctx.registerBean(ServiceDiscovery.class, microservices::discovery);
                  ctx.refresh();
                  ctx.start();
                  return ctx;
                }));
    return Mono.defer(() -> ctxMono).then();
  }

  @Override
  public Mono<Microservices> shutDown(Microservices microservices) {
    return Mono.fromCallable(
        () -> {
          this.ctx.updateAndGet(
              ctx -> {
                ctx.stop();
                ctx.close();
                return ctx;
              });
          return microservices;
        });
  }
}
