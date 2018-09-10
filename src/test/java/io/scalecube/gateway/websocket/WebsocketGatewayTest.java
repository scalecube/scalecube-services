package io.scalecube.gateway.websocket;

import io.scalecube.gateway.examples.GreetingRequest;
import io.scalecube.gateway.examples.GreetingResponse;
import io.scalecube.gateway.examples.GreetingService;
import io.scalecube.gateway.examples.GreetingServiceImpl;
import io.scalecube.services.exceptions.InternalServiceException;
import io.scalecube.services.exceptions.ServiceUnavailableException;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import reactor.test.StepVerifier;

class WebsocketGatewayTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(3);

  @RegisterExtension
  static WebsocketGatewayExtension extension =
      new WebsocketGatewayExtension(new GreetingServiceImpl());

  private GreetingService service;

  @BeforeEach
  void initService() {
    service = extension.client().forService(GreetingService.class);
  }

  @Test
  void shouldReturnSingleResponseWithSimpleRequest() {
    StepVerifier.create(service.one("hello"))
        .expectNext("Echo:hello")
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
  void shouldReturnManyResponsesWithSimpleRequest() {
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
                throwable instanceof ServiceUnavailableException
                    && throwable.getMessage().startsWith("No reachable member with such service"))
        .verify(TIMEOUT);
  }

  @Test
  void shouldReturnErrorDataWhenServiceFails() {
    StepVerifier.create(service.failingOne("hello"))
        .expectErrorMatches(throwable -> throwable instanceof InternalServiceException)
        .verify(TIMEOUT);
  }

  @Test
  void shouldReturnErrorDataWhenRequestDataIsEmpty() {
    StepVerifier.create(service.one(null))
        .expectErrorMatches(
            throwable ->
                "Expected service request data of type: class java.lang.String, but received: null"
                    .equals(throwable.getMessage()))
        .verify(TIMEOUT);
  }
}
