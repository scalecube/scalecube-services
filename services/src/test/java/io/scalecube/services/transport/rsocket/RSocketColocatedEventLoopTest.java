package io.scalecube.services.transport.rsocket;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.scalecube.services.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.annotations.Inject;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class RSocketColocatedEventLoopTest {

  private Microservices ping;
  private Microservices pong;
  private Microservices gateway;

  @BeforeEach
  public void setUp() {
    this.gateway =
        Microservices.start(
            new Context()
                .discovery(
                    serviceEndpoint ->
                        new ScalecubeServiceDiscovery()
                            .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                            .options(opts -> opts.metadata(serviceEndpoint)))
                .transport(RSocketServiceTransport::new));

    final Address gatewayAddress = this.gateway.discoveryAddress();

    Microservices facade =
        Microservices.start(
            new Context()
                .discovery(
                    endpoint ->
                        new ScalecubeServiceDiscovery()
                            .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                            .options(opts -> opts.metadata(endpoint))
                            .membership(cfg -> cfg.seedMembers(gatewayAddress.toString())))
                .transport(RSocketServiceTransport::new)
                .services(new Facade()));

    final Address facadeAddress = facade.discoveryAddress();

    this.ping =
        Microservices.start(
            new Context()
                .discovery(
                    endpoint ->
                        new ScalecubeServiceDiscovery()
                            .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                            .options(opts -> opts.metadata(endpoint))
                            .membership(cfg -> cfg.seedMembers(facadeAddress.toString())))
                .transport(RSocketServiceTransport::new)
                .services((PingService) () -> Mono.just(Thread.currentThread().getName())));

    this.pong =
        Microservices.start(
            new Context()
                .discovery(
                    endpoint ->
                        new ScalecubeServiceDiscovery()
                            .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                            .options(opts -> opts.metadata(endpoint))
                            .membership(cfg -> cfg.seedMembers(facadeAddress.toString())))
                .transport(RSocketServiceTransport::new)
                .services((PongService) () -> Mono.just(Thread.currentThread().getName())));
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
    Optional.ofNullable(gateway).ifPresent(Microservices::close);
    Optional.ofNullable(ping).ifPresent(Microservices::close);
    Optional.ofNullable(pong).ifPresent(Microservices::close);
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
