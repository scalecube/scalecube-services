package io.scalecube.services.gateway.websocket;

import static io.scalecube.services.gateway.GatewayErrorMapperImpl.ERROR_MAPPER;

import io.scalecube.services.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.ServiceInfo;
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
import io.scalecube.services.gateway.ErrorService;
import io.scalecube.services.gateway.ErrorServiceImpl;
import io.scalecube.services.gateway.SomeException;
import io.scalecube.services.gateway.client.StaticAddressRouter;
import io.scalecube.services.gateway.client.websocket.WebsocketGatewayClientTransport;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

class WebsocketLocalGatewayTest extends BaseTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(3);

  private static Microservices gateway;
  private static Address gatewayAddress;
  private static StaticAddressRouter router;

  private ServiceCall serviceCall;
  private GreetingService greetingService;
  private ErrorService errorService;

  @BeforeAll
  static void beforeAll() {
    gateway =
        Microservices.start(
            new Context()
                .gateway(
                    () ->
                        new WebsocketGateway.Builder()
                            .id("WS")
                            .serviceCall(call -> call.errorMapper(ERROR_MAPPER))
                            .errorMapper(ERROR_MAPPER)
                            .build())
                .services(new GreetingServiceImpl())
                .services(
                    ServiceInfo.fromServiceInstance(new ErrorServiceImpl())
                        .errorMapper(ERROR_MAPPER)
                        .build()));

    gatewayAddress = gateway.gateway("WS").address();
    router = new StaticAddressRouter(gatewayAddress);
  }

  @BeforeEach
  void beforeEach() {
    serviceCall =
        new ServiceCall()
            .router(router)
            .transport(
                new WebsocketGatewayClientTransport.Builder().address(gatewayAddress).build());
    greetingService = serviceCall.api(GreetingService.class);
    errorService = serviceCall.errorMapper(ERROR_MAPPER).api(ErrorService.class);
  }

  @AfterEach
  void afterEach() {
    if (serviceCall != null) {
      serviceCall.close();
    }
  }

  @AfterAll
  static void afterAll() {
    if (gateway != null) {
      gateway.close();
    }
  }

  @Test
  void shouldReturnSingleResponseWithSimpleRequest() {
    StepVerifier.create(greetingService.one("hello"))
        .expectNext("Echo:hello")
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  void shouldReturnSingleResponseWithSimpleLongDataRequest() {
    String data = new String(new char[500]);
    StepVerifier.create(greetingService.one(data))
        .expectNext("Echo:" + data)
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  void shouldReturnSingleResponseWithPojoRequest() {
    StepVerifier.create(greetingService.pojoOne(new GreetingRequest("hello")))
        .expectNextMatches(response -> "Echo:hello".equals(response.getText()))
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  void shouldReturnListResponseWithPojoRequest() {
    StepVerifier.create(greetingService.pojoList(new GreetingRequest("hello")))
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

    StepVerifier.create(greetingService.many("hello").take(expectedResponseNum))
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

    StepVerifier.create(
            greetingService.pojoMany(new GreetingRequest("hello")).take(expectedResponseNum))
        .expectNextSequence(expected)
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  void shouldReturnErrorDataWhenServiceFails() {
    StepVerifier.create(greetingService.failingOne("hello"))
        .expectErrorMatches(throwable -> throwable instanceof InternalServiceException)
        .verify(TIMEOUT);
  }

  @Test
  void shouldReturnErrorDataWhenRequestDataIsEmpty() {
    StepVerifier.create(greetingService.one(null))
        .expectErrorMatches(
            throwable ->
                "Expected service request data of type: class java.lang.String, but received: null"
                    .equals(throwable.getMessage()))
        .verify(TIMEOUT);
  }

  @Test
  void shouldReturnNoEventOnNeverService() {
    StepVerifier.create(greetingService.neverOne("hi"))
        .expectSubscription()
        .expectNoEvent(Duration.ofSeconds(1))
        .thenCancel()
        .verify();
  }

  @Test
  void shouldReturnOnEmptyGreeting() {
    StepVerifier.create(greetingService.emptyGreeting(new EmptyGreetingRequest()))
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
    StepVerifier.create(serviceCall.requestOne(request, EmptyGreetingResponse.class))
        .expectSubscription()
        .expectNextMatches(resp -> resp.data() instanceof EmptyGreetingResponse)
        .thenCancel()
        .verify();
  }

  @Test
  void shouldReturnSomeExceptionOnFlux() {
    StepVerifier.create(errorService.manyError()).expectError(SomeException.class).verify(TIMEOUT);
  }

  @Test
  void shouldReturnSomeExceptionOnMono() {
    StepVerifier.create(errorService.oneError()).expectError(SomeException.class).verify(TIMEOUT);
  }
}
