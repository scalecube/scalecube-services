package io.scalecube.services.gateway.http;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.services.examples.GreetingRequest;
import io.scalecube.services.examples.GreetingService;
import io.scalecube.services.examples.GreetingServiceCancelCallback;
import io.scalecube.services.examples.GreetingServiceImpl;
import io.scalecube.services.exceptions.InternalServiceException;
import io.scalecube.services.exceptions.ServiceUnavailableException;
import io.scalecube.services.gateway.clientsdk.Client;
import io.scalecube.services.gateway.clientsdk.ClientMessage;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class HttpGatewayTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(3);

  @RegisterExtension static HttpGatewayExtension extension = new HttpGatewayExtension();

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

  @Test
  void shouldClientDisposeCancelServiceCall() throws InterruptedException {
    // Prerequisites
    CountDownLatch cancelCalled = new CountDownLatch(1);

    GreetingServiceCancelCallback serviceWithCancel =
        new GreetingServiceCancelCallback(new GreetingServiceImpl(), cancelCalled::countDown);

    extension.startServices(serviceWithCancel);

    // Call cancellable service
    Client client = extension.client();

    ClientMessage requestMessage =
        ClientMessage.builder().qualifier("/greeting/never/one").data("theparameter").build();
    Mono<ClientMessage> requestResponse = client.requestResponse(requestMessage);

    requestResponse.subscribe(null, System.err::println);

    // Close client and make assertions

    Mono.delay(Duration.ofSeconds(1))
        .subscribe(
            null, System.err::println, () -> client.close().subscribe(null, System.err::println));

    boolean await = cancelCalled.await(3, TimeUnit.SECONDS);

    assertTrue(await);
  }
}
