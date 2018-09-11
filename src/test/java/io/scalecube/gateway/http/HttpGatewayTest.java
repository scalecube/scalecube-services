package io.scalecube.gateway.http;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.scalecube.gateway.examples.GreetingRequest;
import io.scalecube.gateway.examples.GreetingService;
import io.scalecube.gateway.examples.GreetingServiceImpl;
import io.scalecube.services.exceptions.InternalServiceException;
import io.scalecube.services.exceptions.ServiceUnavailableException;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import reactor.test.StepVerifier;

class HttpGatewayTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(3);

  @RegisterExtension
  static HttpGatewayExtension extension = new HttpGatewayExtension(new GreetingServiceImpl());

  private GreetingService service;

  @BeforeEach
  void initService() {
    service = extension.client().forService(GreetingService.class);
  }

  @Test
  void shouldReturnSingleResponseWithSimpleRequest() {
    StepVerifier.create(service.one("hello")).expectNext("Echo:hello").verifyComplete();
  }

  @Test
  void shouldReturnSingleResponseWithPojoRequest() {
    StepVerifier.create(service.pojoOne(new GreetingRequest("hello")))
        .expectNextMatches(response -> "Echo:hello".equals(response.getText()))
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  void shouldReturnNoContentWhenResponseIsEmpty() {
    StepVerifier.create(service.emptyOne("hello")).expectComplete().verify(TIMEOUT);
  }

  @Test
  void shouldReturnServiceUnavailableWhenServiceIsDown() {
    extension.shutdownServices();

    StepVerifier.create(service.one("hello"))
        .expectErrorSatisfies(
            throwable -> {
              assertEquals(ServiceUnavailableException.class, throwable.getClass());
              assertThat(
                  throwable.getMessage(), startsWith("No reachable member with such service:"));
            })
        .verify(TIMEOUT);
  }

  @Test
  void shouldReturnInternalServerErrorWhenServiceFails() {
    StepVerifier.create(service.failingOne("hello"))
        .expectErrorSatisfies(
            throwable -> {
              assertEquals(InternalServiceException.class, throwable.getClass());
              assertEquals("hello", throwable.getMessage());
            })
        .verify(TIMEOUT);
  }

  @Test
  void shouldReturnInternalServerErrorWhenTimeoutReached() {
    Duration timeout = Duration.ofSeconds(11);

    StepVerifier.create(service.neverOne("hello"))
        .expectErrorSatisfies(
            throwable -> {
              assertEquals(InternalServiceException.class, throwable.getClass());
              assertThat(
                  throwable.getMessage(),
                  startsWith("Did not observe any item or terminal signal"));
            })
        .verify(timeout);
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
