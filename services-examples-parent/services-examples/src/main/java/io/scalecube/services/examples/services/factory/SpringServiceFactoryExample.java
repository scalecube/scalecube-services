package io.scalecube.services.examples.services.factory;

import io.scalecube.services.Microservices;
import io.scalecube.services.MicroservicesContext;
import io.scalecube.services.RemoteService;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.ServiceDefinition;
import io.scalecube.services.ServiceFactory;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.examples.helloworld.service.GreetingServiceImpl;
import io.scalecube.services.examples.services.factory.service.BidiGreetingImpl;
import io.scalecube.services.examples.services.factory.service.api.BidiGreetingService;
import io.scalecube.services.examples.services.factory.service.api.GreetingsService;
import io.scalecube.services.inject.ScalecubeServiceFactory;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;

import java.util.Collection;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

public class SpringServiceFactoryExample {

  /**
   * Main method.
   *
   * @param args - program arguments
   */
  public static void main(String[] args) {
    ServiceFactory serviceFactory2 = ScalecubeServiceFactory.from(new GreetingServiceImpl());

    Microservices service2Node =
        Microservices.builder()
            .serviceFactory(serviceFactory2)
            .discovery(ScalecubeServiceDiscovery::new)
            .transport(RSocketServiceTransport::new)
            .startAwait();

    ServiceFactory serviceFactory1 =
        AnnotatedSpringServiceFactory.configurations(ExampleConfiguration.class);

    Microservices service1Node =
        Microservices.builder()
            .discovery(
                endpoint ->
                    new ScalecubeServiceDiscovery(endpoint)
                        .membership(cfg -> cfg.seedMembers(service2Node.discovery().address())))
            .serviceFactory(serviceFactory1)
            .transport(RSocketServiceTransport::new)
            .startAwait();

    service1Node
        .call()
        .api(BidiGreetingService.class)
        .greeting()
        .log("receive     |")
        .log()
        .log("complete    |")
        .block();

    Mono.whenDelayError(service1Node.shutdown(), service2Node.shutdown()).block();
  }

  @Configuration
  static class ExampleConfiguration {

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    @Bean
    public GreetingsService greetingService(ServiceCall serviceCall) {
      return serviceCall.api(GreetingsService.class);
    }

    @Bean
    public BidiGreetingService bidiGreetingService(GreetingsService greetingsService) {
      return new BidiGreetingImpl(greetingsService);
    }
  }

  private static class AnnotatedSpringServiceFactory implements ServiceFactory {

    private final AnnotationConfigApplicationContext context;
    private final Class<?>[] configuration;

    public static ServiceFactory configurations(Class<?>... configurations) {
      return new AnnotatedSpringServiceFactory(configurations);
    }

    private AnnotatedSpringServiceFactory(Class<?>... configurations) {
      this.context = new AnnotationConfigApplicationContext();
      this.configuration = configurations;
    }

    @Override
    public Mono<? extends Collection<ServiceDefinition>> getServiceDefinitions(
        MicroservicesContext microservices) {
      return Mono.fromCallable(
          () -> {
            this.context.registerBean(MicroservicesContext.class, () -> microservices);
            this.context.registerBean(ServiceCall.class, microservices::serviceCall);
            this.context.registerBean(ServiceDiscovery.class, microservices::serviceDiscovery);
            this.context.register(this.configuration);
            this.context.refresh();
            return this.context.getBeansWithAnnotation(Service.class).values().stream()
                .map(Object::getClass)
                .filter(Predicate.not(RemoteService.class::isAssignableFrom))
                .map(ServiceDefinition::new)
                .collect(Collectors.toList());
          });
    }

    @Override
    public Mono<? extends Collection<ServiceInfo>> initializeServices(
        MicroservicesContext microservices) {
      return Mono.fromCallable(
          () -> {
            this.context.start();
            return this.context.getBeansWithAnnotation(Service.class).values().stream()
                .filter(
                    Predicate.not(bean -> RemoteService.class.isAssignableFrom(bean.getClass())))
                .map(bean -> ServiceInfo.fromServiceInstance(bean).build())
                .collect(Collectors.toList());
          });
    }

    @Override
    public Mono<Void> shutdownServices(MicroservicesContext microservices) {
      return Mono.fromCallable(
          () -> {
            this.context.stop();
            return this.context;
          })
          .then();
    }
  }
}
