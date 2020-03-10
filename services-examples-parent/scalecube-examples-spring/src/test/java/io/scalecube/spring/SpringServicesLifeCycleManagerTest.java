package io.scalecube.spring;

import io.scalecube.services.ScaleCube;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class SpringServicesLifeCycleManagerTest {

  private static ScaleCube microserviceWithSpring;
  private static ScaleCube microserviceWithoutSpring;

  @BeforeAll
  public static void setup() {
    Hooks.onOperatorDebug();
    microserviceWithSpring = microserviceWithSpring();
    microserviceWithoutSpring = microserviceWithoutSpring();
  }

  @AfterAll
  public static void tearDown() {
    try {
      microserviceWithSpring.shutdown().block();
    } catch (Exception ignore) {
      // no-op
    }

    try {
      microserviceWithoutSpring.shutdown().block();
    } catch (Exception ignore) {
      // no-op
    }
  }

  private static ScaleCube microserviceWithSpring() {
    return ScaleCube.builder()
        .discovery(ScalecubeServiceDiscovery::new)
        .transport(RSocketServiceTransport::new)
        .services(new SpringServicesLifeCycleManager(Beans.class))
        .startAwait();
  }

  private static ScaleCube microserviceWithoutSpring() {
    return ScaleCube.builder()
        .discovery(SpringServicesLifeCycleManagerTest::serviceDiscovery)
        .transport(RSocketServiceTransport::new)
        .services((SimpleService) () -> Mono.just(1L))
        .startAwait();
  }

  @Test
  public void test_remote_greeting_request_completes_before_timeout() {

    LocalService service = microserviceWithSpring.call().api(LocalService.class);

    // call the service.
    Mono<Long> result = service.get();
    Long block = result.block(Duration.ofSeconds(10));
    assertEquals(-1L, block.longValue());
  }

  private static ServiceDiscovery serviceDiscovery(ServiceEndpoint endpoint) {
    return new ScalecubeServiceDiscovery(endpoint)
        .membership(cfg -> cfg.seedMembers(microserviceWithSpring.discovery().address()));
  }

  @Configuration
  static class Beans {
    @Bean
    public LocalService localServiceBean(ServiceCall serviceCall, ServiceDiscovery serviceDiscovery) {
      return new LocalServiceBean(serviceCall, serviceDiscovery);
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
    Mono<Long> get();
  }

  public static class LocalServiceBean implements LocalService {

    private final SimpleService serviceCall;

    public LocalServiceBean(ServiceCall serviceCall, ServiceDiscovery serviceDiscovery) {
      this.serviceCall = serviceCall.api(SimpleService.class);
      this.serviceCall.get().subscribe(System.out::println);
      serviceDiscovery.listenDiscovery().subscribe(System.out::println);
    }

    public Mono<Long> get() {
      return serviceCall.get().map(n -> n * -1);
    }
  }
}
