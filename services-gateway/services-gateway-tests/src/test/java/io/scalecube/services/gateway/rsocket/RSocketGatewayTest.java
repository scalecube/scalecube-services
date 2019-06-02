package io.scalecube.services.gateway.rsocket;

import io.rsocket.exceptions.ApplicationErrorException;
import io.scalecube.services.examples.GreetingRequest;
import io.scalecube.services.examples.GreetingResponse;
import io.scalecube.services.examples.GreetingService;
import io.scalecube.services.examples.GreetingServiceImpl;
import io.scalecube.services.exceptions.InternalServiceException;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class RSocketGatewayTest {

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
    StepVerifier.create(service.one("hello"))
        .expectNext("Echo:hello")
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  void shouldReturnSingleResponseWithSimpleLongDataRequest() {
    String data = new String(new char[500]);
    StepVerifier.create(service.one(data))
        .expectNext("Echo:" + data)
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  void shouldReturnSingleResponseWithPojoRequest() {
    StepVerifier.create(service.pojoOne(new GreetingRequest("hello")))
        .expectNextMatches(response -> "Echo:hello".equals(response.getText()))
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  void shouldReturnManyResponses() {
    int expectedResponseNum = 3;
    List<String> expected =
        IntStream.range(0, expectedResponseNum)
            .mapToObj(i -> "Greeting (" + i + ") to: hello")
            .collect(Collectors.toList());

    StepVerifier.create(service.many("hello").take(expectedResponseNum))
        .expectNextSequence(expected)
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  void shouldReturnManyResponsesWithPojoRequest() {
    int expectedResponseNum = 3;
    List<GreetingResponse> expected =
        IntStream.range(0, expectedResponseNum)
            .mapToObj(i -> new GreetingResponse("Greeting (" + i + ") to: hello"))
            .collect(Collectors.toList());

    StepVerifier.create(service.pojoMany(new GreetingRequest("hello")).take(expectedResponseNum))
        .expectNextSequence(expected)
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  void shouldReturnExceptionWhenServiceIsDown() {
    extension.shutdownServices();

    StepVerifier.create(service.one("hello"))
        .expectErrorMatches(
            throwable ->
                throwable instanceof ApplicationErrorException
                    && throwable.getMessage().startsWith("No reachable member with such service"))
        .verify(TIMEOUT);
  }

  @Test
  void shouldReturnErrorDataWhenServiceFails() {
    String req = "hello";
    Mono<String> result = service.failingOne(req);

    StepVerifier.create(result)
        .expectErrorMatches(throwable -> throwable instanceof InternalServiceException)
        .verify(TIMEOUT);
  }

  @Test
  void shouldReturnErrorDataWhenRequestDataIsEmpty() {
    Mono<String> result = service.one(null);
    StepVerifier.create(result)
        .expectErrorMatches(
            throwable ->
                "Expected service request data of type: class java.lang.String, but received: null"
                    .equals(throwable.getMessage()))
        .verify(TIMEOUT);
  }

  @Test
  void shouldSuccessfullyReuseServiceProxy() {
    StepVerifier.create(service.one("hello"))
        .expectNext("Echo:hello")
        .expectComplete()
        .verify(TIMEOUT);

    StepVerifier.create(service.one("hello"))
        .expectNext("Echo:hello")
        .expectComplete()
        .verify(TIMEOUT);
  }
}
