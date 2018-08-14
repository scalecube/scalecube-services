package io.scalecube.gateway.rsocket;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.scalecube.gateway.examples.GreetingService;
import io.scalecube.gateway.examples.GreetingServiceImpl;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;

public class RSocketWebsocketServerTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(3);

  @RegisterExtension
  private static RsocketGatewayExtension extention = new RsocketGatewayExtension(new GreetingServiceImpl());

  private static GreetingService service;

  @BeforeEach
  private void initService() {
    service = extention.client().forService(GreetingService.class);
  }

  @Test
  public void shouldReturnSingleResponse() {
    String req = "hello";
    String expected = "Echo:" + req;

    Mono<String> result = service.one(req);

    StepVerifier.create(result)
        .assertNext(resp -> assertEquals(expected, resp))
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  public void shouldReturnManyResponses() {
    Flux<Long> result = service.manyStream(5L);

    StepVerifier.create(result)
        .assertNext(next -> assertEquals(0, (long) next))
        .expectNextCount(4)
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  public void shouldReturnExceptionWhenQualifierIsWrong() {
    extention.shutdownServices();
    Mono<String> result = service.one("hello");
    StepVerifier.create(result)
        .expectErrorMatches(throwable -> throwable.getMessage().startsWith("No reachable member with such service"))
        .verify(TIMEOUT);
    extention.startServices();
  }

  @Test
  public void shouldReturnErrorDataWhenServiceFails() {
    String req = "hello";
    Mono<String> result = service.failingOne(req);

    StepVerifier.create(result)
        .expectErrorMatches(t -> t.getMessage().equals(req))
        .verify(TIMEOUT);
  }

  @Test
  public void shouldReturnErrorDataWhenRequestDataIsEmpty() {
    Mono<String> result = service.one(null);
    StepVerifier.create(result)
        .expectErrorMatches(t -> "Expected service request data of type: class java.lang.String, but received: null"
            .equals(t.getMessage()))
        .verify(TIMEOUT);
  }
}
