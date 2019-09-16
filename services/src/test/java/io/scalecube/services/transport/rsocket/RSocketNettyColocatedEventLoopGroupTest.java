package io.scalecube.services.transport.rsocket;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class RSocketNettyColocatedEventLoopGroupTest {

  private Microservices ping;

  private Microservices pong;

  private Microservices gateway;

  @BeforeEach
  void setUp() {
    this.gateway =
        Microservices.builder()
            .discovery(ScalecubeServiceDiscovery::new)
            .transport(RSocketServiceTransport::new)
            .startAwait();

    this.ping =
        Microservices.builder()
            .discovery(
                serviceEndpoint ->
                    new ScalecubeServiceDiscovery(serviceEndpoint)
                        .options(
                            opt ->
                                opt.membership(
                                    cfg -> cfg.seedMembers(gateway.discovery().address()))))
            .transport(RSocketServiceTransport::new)
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
                                    cfg -> cfg.seedMembers(gateway.discovery().address()))))
            .transport(RSocketServiceTransport::new)
            .services((PongService) () -> Mono.just(Thread.currentThread().getName()))
            .startAwait();
  }

  @Test
  void test() {
    ServiceCall call = gateway.call();
    PingService pingService = call.api(PingService.class);
    PongService pongService = call.api(PongService.class);
    Mono<String> ping = pingService.ping().log();
    Mono<String> pong = pongService.pong().log();

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
}
