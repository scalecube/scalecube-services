package io.scalecube.gateway.rsocket;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.scalecube.gateway.examples.GreetingService;
import io.scalecube.gateway.examples.GreetingServiceImpl;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class RSocketWebsocketGatewayTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(3);

  @RegisterExtension
  static RsocketGatewayExtension extension = new RsocketGatewayExtension(new GreetingServiceImpl());

  private GreetingService service;

  @BeforeEach
  void initService() {
    service = extension.client().forService(GreetingService.class);
  }

  @Test
  void shouldReturnSingleResponse() {
    String req = "hello";
    String expected = "Echo:" + req;

    Mono<String> result = service.one(req);

    StepVerifier.create(result)
        .assertNext(resp -> assertEquals(expected, resp))
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  void shouldReturnManyResponses() {
    Flux<Long> result = service.manyStream(5L);

    StepVerifier.create(result)
        .assertNext(next -> assertEquals(0, (long) next))
        .expectNextCount(4)
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  void shouldReturnExceptionWhenQualifierIsWrong() {
    extension.shutdownServices();
    Mono<String> result = service.one("hello");
    StepVerifier.create(result)
        .expectErrorMatches(
            throwable -> throwable.getMessage().startsWith("No reachable member with such service"))
        .verify(TIMEOUT);
  }

  @Test
  void shouldReturnErrorDataWhenServiceFails() {
    String req = "hello";
    Mono<String> result = service.failingOne(req);

    StepVerifier.create(result).expectErrorMatches(t -> t.getMessage().equals(req)).verify(TIMEOUT);
  }

  @Test
  void shouldReturnErrorDataWhenRequestDataIsEmpty() {
    Mono<String> result = service.one(null);
    StepVerifier.create(result)
        .expectErrorMatches(
            t ->
                "Expected service request data of type: class java.lang.String, but received: null"
                    .equals(t.getMessage()))
        .verify(TIMEOUT);
  }
}
