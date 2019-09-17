package io.scalecube.services.transport.rsocket;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.annotations.Inject;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.transport.api.ServiceTransport;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class RSocketNettyColocatedEventLoopGroupTest {

  private Microservices ping;

  private Microservices pong;

  private Microservices facade;

  private Microservices gateway;

  @BeforeEach
  void setUp() {

    Supplier<ServiceTransport> transport = RSocketServiceTransport::new;
    this.gateway =
        Microservices.builder()
            .discovery(ScalecubeServiceDiscovery::new)
            .transport(transport)
            .startAwait();

    this.facade =
        Microservices.builder()
            .discovery(
                serviceEndpoint ->
                    new ScalecubeServiceDiscovery(serviceEndpoint)
                        .options(
                            opt ->
                                opt.membership(
                                    cfg -> cfg.seedMembers(gateway.discovery().address()))))
            .transport(transport)
            .services(new Facade())
            .startAwait();

    this.ping =
        Microservices.builder()
            .discovery(
                serviceEndpoint ->
                    new ScalecubeServiceDiscovery(serviceEndpoint)
                        .options(
                            opt ->
                                opt.membership(
                                    cfg -> cfg.seedMembers(facade.discovery().address()))))
            .transport(transport)
            .services((PingService) () -> Mono.just(Thread.currentThread().getName()))
            .startAwait();

    this.pong =
        Microservices.builder()
            .discovery(
                serviceEndpoint ->
                    new ScalecubeServiceDiscovery(serviceEndpoint)
                        .options(
                            opt ->
                                opt.membership(
                                    cfg -> cfg.seedMembers(facade.discovery().address()))))
            .transport(transport)
            .services((PongService) () -> Mono.just(Thread.currentThread().getName()))
            .startAwait();
  }

  @Test
  void test() {
    ServiceCall call = gateway.call();

    FacadeService facade = call.api(FacadeService.class);

    Mono<String> ping = facade.call().log();
    Mono<String> pong = facade.call().log();

    StepVerifier.create(Mono.zip(ping, pong))
        .assertNext(t -> assertEquals(t.getT1(), t.getT2()))
        .verifyComplete();
  }

  @AfterEach
  void tearDown() {
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

  class Facade implements FacadeService {

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
