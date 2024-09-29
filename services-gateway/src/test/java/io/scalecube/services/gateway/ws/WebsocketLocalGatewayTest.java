package io.scalecube.services.gateway.ws;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.scalecube.services.api.Qualifier;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.examples.EmptyGreetingRequest;
import io.scalecube.services.examples.EmptyGreetingResponse;
import io.scalecube.services.examples.GreetingRequest;
import io.scalecube.services.examples.GreetingResponse;
import io.scalecube.services.examples.GreetingService;
import io.scalecube.services.examples.GreetingServiceImpl;
import io.scalecube.services.exceptions.InternalServiceException;
import io.scalecube.services.gateway.BaseTest;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import reactor.test.StepVerifier;

class WebsocketLocalGatewayTest extends BaseTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(3);

  @RegisterExtension
  static WebsocketLocalGatewayExtension extension =
      new WebsocketLocalGatewayExtension(new GreetingServiceImpl());

  private GreetingService service;

  @BeforeEach
  void initService() {
    service = extension.client().api(GreetingService.class);
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
  void shouldReturnListResponseWithPojoRequest() {
    StepVerifier.create(service.pojoList(new GreetingRequest("hello")))
        .expectNextMatches(response -> "Echo:hello".equals(response.get(0).getText()))
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

  @Test
  void shouldReturnNoEventOnNeverService() {
    StepVerifier.create(service.neverOne("hi"))
        .expectSubscription()
        .expectNoEvent(Duration.ofSeconds(1))
        .thenCancel()
        .verify();
  }

  @Test
  void shouldReturnOnEmptyGreeting() {
    StepVerifier.create(service.emptyGreeting(new EmptyGreetingRequest()))
        .expectSubscription()
        .expectNextMatches(resp -> resp instanceof EmptyGreetingResponse)
        .thenCancel()
        .verify();
  }

  @Test
  void shouldReturnOnEmptyMessageGreeting() {
    String qualifier = Qualifier.asString(GreetingService.NAMESPACE, "empty/wrappedPojo");
    ServiceMessage request =
        ServiceMessage.builder().qualifier(qualifier).data(new EmptyGreetingRequest()).build();
    StepVerifier.create(extension.client().requestOne(request, EmptyGreetingResponse.class))
        .expectSubscription()
        .expectNextMatches(resp -> resp.data() instanceof EmptyGreetingResponse)
        .thenCancel()
        .verify();
  }

  @Test
  public void testManyStreamBlockFirst() {
    for (int i = 0; i < 100; i++) {
      //noinspection ConstantConditions
      long first = service.manyStream(30L).filter(k -> k != 0).take(1).blockFirst();
      assertEquals(1, first);
    }
  }
}
