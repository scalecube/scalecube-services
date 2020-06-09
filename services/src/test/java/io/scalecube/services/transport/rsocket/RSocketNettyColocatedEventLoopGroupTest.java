package io.scalecube.services.transport.rsocket;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.scalecube.services.BaseTest;
import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.annotations.Inject;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import io.scalecube.services.inject.ScalecubeServiceFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class RSocketNettyColocatedEventLoopGroupTest extends BaseTest {

  private Microservices ping;
  private Microservices pong;
  private Microservices gateway;

  @BeforeEach
  public void setUp() {
    this.gateway =
        Microservices.builder()
            .discovery(ScalecubeServiceDiscovery::new)
            .transport(RSocketServiceTransport::new)
            .startAwait();

    Microservices facade =
        Microservices.builder()
            .discovery(
                endpoint ->
                    new ScalecubeServiceDiscovery(endpoint)
                        .membership(cfg -> cfg.seedMembers(gateway.discovery().address())))
            .transport(RSocketServiceTransport::new)
            .serviceFactory(ScalecubeServiceFactory.fromInstances(new Facade()))
            .startAwait();

    PingService pingService = () -> Mono.just(Thread.currentThread().getName());
    this.ping =
        Microservices.builder()
            .discovery(
                endpoint ->
                    new ScalecubeServiceDiscovery(endpoint)
                        .membership(cfg -> cfg.seedMembers(facade.discovery().address())))
            .transport(RSocketServiceTransport::new)
            .serviceFactory(ScalecubeServiceFactory.fromInstances(pingService))
            .startAwait();

    PongService pongService = () -> Mono.just(Thread.currentThread().getName());
    this.pong =
        Microservices.builder()
            .discovery(
                endpoint ->
                    new ScalecubeServiceDiscovery(endpoint)
                        .membership(cfg -> cfg.seedMembers(facade.discovery().address())))
            .transport(RSocketServiceTransport::new)
            .serviceFactory(ScalecubeServiceFactory.fromInstances(pongService))
            .startAwait();
  }

  @Test
  public void testColocatedEventLoopGroup() {
    ServiceCall call = gateway.call();

    FacadeService facade = call.api(FacadeService.class);

    Mono<String> ping = facade.call().log();
    Mono<String> pong = facade.call().log();

    StepVerifier.create(Mono.zip(ping, pong))
        .assertNext(t -> assertEquals(t.getT1(), t.getT2()))
        .verifyComplete();
  }

  @AfterEach
  public void tearDown() {
    try {
      Mono.whenDelayError(
              Optional.ofNullable(gateway).map(Microservices::shutdown).orElse(Mono.empty()),
              Optional.ofNullable(ping).map(Microservices::shutdown).orElse(Mono.empty()),
              Optional.ofNullable(pong).map(Microservices::shutdown).orElse(Mono.empty()))
          .block();
    } catch (Throwable ignore) {
      // no-op
    }
  }

  @Service("facade")
  public interface FacadeService {

    @ServiceMethod("call")
    Mono<String> call();
  }

  @Service("service-ping")
  public interface PingService {

    @ServiceMethod("ping")
    Mono<String> ping();
  }

  @Service("service-pong")
  public interface PongService {

    @ServiceMethod("pong")
    Mono<String> pong();
  }

  public static class Facade implements FacadeService {

    private final AtomicBoolean callPing = new AtomicBoolean();
    @Inject private PingService pingService;
    @Inject private PongService pongService;

    @Override
    public Mono<String> call() {
      return (callPing.getAndSet(!callPing.get()) ? pingService.ping() : pongService.pong())
          .map(ignore -> Thread.currentThread().getName());
    }
  }
}
