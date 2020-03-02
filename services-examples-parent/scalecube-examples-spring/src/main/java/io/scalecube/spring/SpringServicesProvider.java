package io.scalecube.spring;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.ServicesProvider;
import io.scalecube.services.annotations.Service;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class SpringServicesProvider implements ServicesProvider {

  private final BiFunction<
          Microservices, AnnotationConfigApplicationContext, AnnotationConfigApplicationContext>
      ctxFactory;

  private final AtomicReference<AnnotationConfigApplicationContext> ctx = new AtomicReference<>();

  public SpringServicesProvider(Class<?>... configurations) {
    this.ctxFactory =
        (microservices, ctx) -> {
          AnnotationConfigApplicationContext springContext;
          if (Objects.isNull(ctx)) {
            AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
            context.registerBean(ServiceCall.class, microservices::call);
            Stream.of(configurations).forEach(context::register);
            springContext = context;
          } else {
            springContext = ctx;
          }
          springContext.refresh();
          springContext.start();
          return springContext;
        };
  }

  @Override
  public Mono<Collection<ServiceInfo>> provide(Microservices microservices) {
    AnnotationConfigApplicationContext context =
        this.ctx.updateAndGet(c -> ctxFactory.apply(microservices, c));
    List<ServiceInfo> beans =
        context.getBeansWithAnnotation(Service.class).values().stream()
            .map(bean -> ServiceInfo.fromServiceInstance(bean).build())
            .collect(Collectors.toList());
    return Mono.just(beans);
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
