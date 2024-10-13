package io.scalecube.services.gateway.websocket;

import io.scalecube.services.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.gateway.BaseTest;
import io.scalecube.services.gateway.TestGatewaySessionHandler;
import io.scalecube.services.gateway.client.StaticAddressRouter;
import io.scalecube.services.gateway.client.websocket.WebsocketGatewayClientTransport;
import java.time.Duration;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

class WebsocketServerTest extends BaseTest {

  private static Microservices gateway;
  private static Address gatewayAddress;

  @BeforeAll
  static void beforeAll() {
    gateway =
        Microservices.start(
            new Context()
                .gateway(
                    () ->
                        new WebsocketGateway.Builder()
                            .id("WS")
                            .gatewayHandler(new TestGatewaySessionHandler())
                            .build())
                .services(new TestServiceImpl()));

    gatewayAddress = gateway.gateway("WS").address();
  }

  @AfterAll
  static void afterAll() {
    if (gateway != null) {
      gateway.close();
    }
  }

  @Test
  void testMessageSequence() {
    try (ServiceCall serviceCall =
        new ServiceCall()
            .transport(
                new WebsocketGatewayClientTransport.Builder().address(gatewayAddress).build())
            .router(new StaticAddressRouter(gatewayAddress))) {
      int count = 1000;
      StepVerifier.create(serviceCall.api(TestService.class).many(count) /*.log("<<<")*/)
          .expectNextSequence(IntStream.range(0, count).boxed().collect(Collectors.toList()))
          .expectComplete()
          .verify(Duration.ofSeconds(10));
    }
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
