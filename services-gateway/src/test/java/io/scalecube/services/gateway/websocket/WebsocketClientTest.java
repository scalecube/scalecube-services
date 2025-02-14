package io.scalecube.services.gateway.websocket;

import static io.scalecube.services.gateway.GatewayErrorMapperImpl.ERROR_MAPPER;

import io.scalecube.services.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.gateway.ErrorService;
import io.scalecube.services.gateway.ErrorServiceImpl;
import io.scalecube.services.gateway.SomeException;
import io.scalecube.services.gateway.client.websocket.WebsocketGatewayClientTransport;
import io.scalecube.services.routing.StaticAddressRouter;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.time.Duration;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

class WebsocketClientTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(10);

  private static Microservices gateway;
  private static Address gatewayAddress;
  private static Microservices microservices;

  @BeforeAll
  static void beforeAll() {
    gateway =
        Microservices.start(
            new Context()
                .discovery(
                    serviceEndpoint ->
                        new ScalecubeServiceDiscovery()
                            .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                            .options(opts -> opts.metadata(serviceEndpoint)))
                .transport(RSocketServiceTransport::new)
                .gateway(
                    () ->
                        new WebsocketGateway.Builder()
                            .id("WS")
                            .gatewayHandler(new TestGatewaySessionHandler())
                            .build()));

    gatewayAddress = gateway.gateway("WS").address();

    microservices =
        Microservices.start(
            new Context()
                .discovery(
                    serviceEndpoint ->
                        new ScalecubeServiceDiscovery()
                            .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                            .options(opts -> opts.metadata(serviceEndpoint))
                            .membership(
                                opts -> opts.seedMembers(gateway.discoveryAddress().toString())))
                .transport(RSocketServiceTransport::new)
                .services(new TestServiceImpl())
                .services(
                    ServiceInfo.fromServiceInstance(new ErrorServiceImpl())
                        .errorMapper(ERROR_MAPPER)
                        .build()));
  }

  @AfterAll
  static void afterAll() {
    gateway.close();
    microservices.close();
  }

  @Test
  void testMessageSequence() {
    try (ServiceCall serviceCall = serviceCall(gatewayAddress)) {
      final int count = 1000;
      StepVerifier.create(serviceCall.api(TestService.class).many(count) /*.log("<<<")*/)
          .expectNextSequence(IntStream.range(0, count).boxed().collect(Collectors.toList()))
          .expectComplete()
          .verify(TIMEOUT);
    }
  }

  @Test
  void shouldReturnSomeExceptionOnFlux() {
    try (final ServiceCall serviceCall = serviceCall(gatewayAddress)) {
      final ErrorService errorService =
          serviceCall.errorMapper(ERROR_MAPPER).api(ErrorService.class);
      StepVerifier.create(errorService.manyError())
          .expectError(SomeException.class)
          .verify(TIMEOUT);
    }
  }

  @Test
  void shouldReturnSomeExceptionOnMono() {
    try (final ServiceCall serviceCall = serviceCall(gatewayAddress)) {
      final ErrorService errorService =
          serviceCall.errorMapper(ERROR_MAPPER).api(ErrorService.class);
      StepVerifier.create(errorService.oneError()).expectError(SomeException.class).verify(TIMEOUT);
    }
  }

  private static ServiceCall serviceCall(final Address address) {
    return new ServiceCall()
        .transport(new WebsocketGatewayClientTransport.Builder().address(address).build())
        .router(StaticAddressRouter.fromAddress(address));
  }

  @Service
  public interface TestService {

    @ServiceMethod
    Flux<Integer> many(int count);
  }

  private static class TestServiceImpl implements TestService {

    @Override
    public Flux<Integer> many(int count) {
      return Flux.using(
          ReactiveAdapter::new,
          reactiveAdapter ->
              reactiveAdapter
                  .receive()
                  .take(count)
                  .cast(Integer.class)
                  .doOnSubscribe(
                      s ->
                          new Thread(
                                  () -> {
                                    for (int i = 0; ; ) {
                                      int r = (int) reactiveAdapter.requested(100);

                                      if (reactiveAdapter.isFastPath()) {
                                        try {
                                          if (reactiveAdapter.isDisposed()) {
                                            return;
                                          }
                                          reactiveAdapter.tryNext(i++);
                                          reactiveAdapter.incrementProduced();
                                        } catch (Throwable e) {
                                          reactiveAdapter.lastError(e);
                                          return;
                                        }
                                      } else if (r > 0) {
                                        try {
                                          if (reactiveAdapter.isDisposed()) {
                                            return;
                                          }
                                          reactiveAdapter.tryNext(i++);
                                          reactiveAdapter.incrementProduced();
                                        } catch (Throwable e) {
                                          reactiveAdapter.lastError(e);
                                          return;
                                        }

                                        reactiveAdapter.commitProduced();
                                      }
                                    }
                                  })
                              .start()),
          ReactiveAdapter::dispose);
    }
  }
}
