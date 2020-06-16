package io.scalecube.services.examples.services.factory;

import io.scalecube.services.Microservices;
import io.scalecube.services.MicroservicesContext;
import io.scalecube.services.ScalecubeServiceFactory;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.ServiceDefinition;
import io.scalecube.services.ServiceFactory;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.examples.helloworld.service.GreetingServiceImpl;
import io.scalecube.services.examples.services.factory.service.BidiGreetingImpl;
import io.scalecube.services.examples.services.factory.service.api.BidiGreetingService;
import io.scalecube.services.examples.services.factory.service.api.GreetingsService;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanDefinitionCustomizer;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.ResolvableType;
import reactor.core.publisher.Mono;

public class LightweightSpringServiceFactoryExample {

  /**
   * Main method.
   *
   * @param args - program arguments
   */
  public static void main(String[] args) {
    ServiceFactory serviceFactory2 =
        ScalecubeServiceFactory.fromInstances(new GreetingServiceImpl());

    Microservices service2Node =
        Microservices.builder()
            .serviceFactory(serviceFactory2)
            .discovery("s2", ScalecubeServiceDiscovery::new)
            .transport(RSocketServiceTransport::new)
            .startAwait();

    ServiceFactory serviceFactory1 =
        LightweightSpringServiceFactory.initialize(
            ctx -> {
              ctx.registerBean(
                  GreetingsService.class,
                  () -> ctx.getBean(ServiceCall.class).api(GreetingsService.class));

              ctx.registerBean(BidiGreetingImpl.class, Customizers.LOCAL);
            });

    Microservices service1Node =
        Microservices.builder()
            .discovery(
                "s1",
                endpoint ->
                    new ScalecubeServiceDiscovery(endpoint)
                        .membership(
                            cfg ->
                                cfg.seedMembers(service2Node.context().discovery("s2").address())))
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

  private static class LightweightSpringServiceFactory implements ServiceFactory {

    private final GenericApplicationContext context;
    private final List<String> localServiceBeanNames = new ArrayList<>();

    public LightweightSpringServiceFactory(
        ApplicationContextInitializer<GenericApplicationContext> initializer) {
      this.context = new GenericApplicationContext();
      initializer.initialize(this.context);
    }

    public static ServiceFactory initialize(
        ApplicationContextInitializer<GenericApplicationContext> initializer) {
      return new LightweightSpringServiceFactory(initializer);
    }

    @Override
    public Collection<ServiceDefinition> getServiceDefinitions() {
      return Stream.of(this.context.getBeanDefinitionNames())
          .map(
              beanName -> {
                BeanDefinition bd = this.context.getBeanDefinition(beanName);
                bd.setAttribute("name", beanName);
                return bd;
              })
          .filter(Customizers.IS_LOCAL)
          .peek(bd -> this.localServiceBeanNames.add((String) bd.getAttribute("name")))
          .map(BeanDefinition::getResolvableType)
          .map(ResolvableType::resolve)
          .map(ServiceDefinition::new)
          .collect(Collectors.toList());
    }

    @Override
    public Mono<? extends Collection<ServiceInfo>> initializeServices(
        MicroservicesContext microservices) {
      return Mono.fromCallable(
          () -> {
            this.context.registerBean(ServiceCall.class, microservices::serviceCall);
            this.context.refresh();
            this.context.start();
            return this.localServiceBeanNames.stream()
                .map(this.context::getBean)
                .map(bean -> ServiceInfo.fromServiceInstance(bean).build())
                .collect(Collectors.toList());
          });
    }

    @Override
    public Mono<Void> shutdownServices(MicroservicesContext microservices) {
      return Mono.fromRunnable(this.context::stop);
    }
  }

  private static class Customizers {

    public static final BeanDefinitionCustomizer LOCAL = bd -> bd.setAttribute("_LOCAL", true);
    public static final Predicate<BeanDefinition> IS_LOCAL =
        bd -> bd.hasAttribute("_LOCAL") && (boolean) bd.getAttribute("_LOCAL");
  }
}
