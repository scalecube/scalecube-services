package io.scalecube.services.examples.services.factory;

import static org.springframework.beans.factory.config.BeanDefinition.SCOPE_PROTOTYPE;

import io.scalecube.services.Microservices;
import io.scalecube.services.MicroservicesContext;
import io.scalecube.services.Reflect;
import io.scalecube.services.ScalecubeServiceFactory;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.ServiceDefinition;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceFactory;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.examples.helloworld.service.GreetingServiceImpl;
import io.scalecube.services.examples.services.factory.service.BidiGreetingImpl;
import io.scalecube.services.examples.services.factory.service.api.BidiGreetingService;
import io.scalecube.services.examples.services.factory.service.api.GreetingsService;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.ResolvableType;
import org.springframework.core.annotation.AliasFor;
import reactor.core.publisher.Mono;

public class SpringServiceFactoryExample {

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
    @Lazy
    public GreetingsService greetingService(ServiceCall serviceCall) {
      return serviceCall.api(GreetingsService.class);
    }

    @ScalecubeBean
    public BidiGreetingService bidiGreetingService(GreetingsService greetingsService) {
      return new BidiGreetingImpl(greetingsService);
    }
  }

  @Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
  @Retention(RetentionPolicy.RUNTIME)
  @Bean
  @Lazy
  @interface ScalecubeBean {

    /**
     * {@link Bean#value()}.
     *
     * @return name
     */
    @AliasFor(annotation = Bean.class, attribute = "name")
    String[] value() default {};

    /**
     * {@link Bean#name()}.
     *
     * @return name
     */
    @AliasFor(annotation = Bean.class, attribute = "value")
    String[] name() default {};

    /**
     * {@link Bean#initMethod()} ()}.
     *
     * @return init method
     */
    @AliasFor(annotation = Bean.class, attribute = "initMethod")
    String initMethod() default "";

    /**
     * {@link Bean#destroyMethod()} ()}.
     *
     * @return destroy method
     */
    @AliasFor(annotation = Bean.class, attribute = "destroyMethod")
    String destroyMethod() default AbstractBeanDefinition.INFER_METHOD;
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
    public Collection<ServiceDefinition> getServiceDefinitions() {
      this.context.register(this.configuration);
      this.context.refresh();
      String[] beanNames = this.context.getBeanNamesForAnnotation(ScalecubeBean.class);
      return Stream.of(beanNames)
          .map(this.context::getBeanDefinition)
          .map(BeanDefinition::getResolvableType)
          .map(ResolvableType::resolve)
          .filter(Objects::nonNull)
          .filter(Reflect::isService)
          .map(ServiceDefinition::new)
          .collect(Collectors.toList());
    }

    @Override
    public Mono<? extends Collection<ServiceInfo>> initializeServices(
        MicroservicesContext microservices) {
      return Mono.fromCallable(
          () -> {
            this.context.registerBean(MicroservicesContext.class, () -> microservices);
            this.context.registerBean(
                ServiceCall.class,
                microservices::serviceCall,
                beanDefinition -> beanDefinition.setScope(SCOPE_PROTOTYPE));
            this.context.registerBean(
                ServiceEndpoint.class,
                microservices::serviceEndpoint,
                beanDefinition -> beanDefinition.setScope(SCOPE_PROTOTYPE));
            this.context.start();
            return this.context.getBeansWithAnnotation(ScalecubeBean.class).values().stream()
                .map(bean -> ServiceInfo.fromServiceInstance(bean).build())
                .collect(Collectors.toList());
          });
    }

    @Override
    public Mono<Void> shutdownServices(MicroservicesContext microservices) {
      return Mono.fromRunnable(this.context::stop).then();
    }
  }
}
