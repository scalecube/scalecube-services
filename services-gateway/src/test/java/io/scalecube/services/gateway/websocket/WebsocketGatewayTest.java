package io.scalecube.services.gateway.websocket;

import static io.scalecube.services.gateway.GatewayErrorMapperImpl.ERROR_MAPPER;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import io.scalecube.services.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.api.Qualifier;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.examples.EmptyGreetingRequest;
import io.scalecube.services.examples.EmptyGreetingResponse;
import io.scalecube.services.examples.GreetingRequest;
import io.scalecube.services.examples.GreetingResponse;
import io.scalecube.services.examples.GreetingService;
import io.scalecube.services.examples.GreetingServiceImpl;
import io.scalecube.services.exceptions.BadRequestException;
import io.scalecube.services.exceptions.InternalServiceException;
import io.scalecube.services.gateway.ErrorService;
import io.scalecube.services.gateway.ErrorServiceImpl;
import io.scalecube.services.gateway.SomeException;
import io.scalecube.services.gateway.client.websocket.WebsocketGatewayClientTransport;
import io.scalecube.services.routing.StaticAddressRouter;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
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

class WebsocketGatewayTest {

  private static Microservices gateway;
  private static Address gatewayAddress;
  private static StaticAddressRouter router;
  private static Microservices microservices;

  private ServiceCall serviceCall;
  private GreetingService greetingService;
  private ErrorService errorService;

  @BeforeAll
  static void beforeAll() {
    StepVerifier.setDefaultTimeout(Duration.ofSeconds(3));

    gateway =
        Microservices.start(
            new Context()
                .discovery(
                    serviceEndpoint ->
                        new ScalecubeServiceDiscovery()
                            .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                            .options(opts -> opts.metadata(serviceEndpoint)))
                .transport(RSocketServiceTransport::new)
                .gateway(() -> WebsocketGateway.builder().id("WS").heartbeatEnabled(true).build()));

    gatewayAddress = gateway.gateway("WS").address();
    router = StaticAddressRouter.forService(gatewayAddress, "app-service").build();

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
                .defaultLogger("microservices")
                .services(new GreetingServiceImpl())
                .services(
                    ServiceInfo.fromServiceInstance(new ErrorServiceImpl())
                        .errorMapper(ERROR_MAPPER)
                        .build()));
  }

  @BeforeEach
  void beforeEach() {
    serviceCall =
        new ServiceCall()
            .router(router)
            .transport(WebsocketGatewayClientTransport.builder().address(gatewayAddress).build());
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
    if (microservices != null) {
      microservices.close();
    }
    StepVerifier.resetDefaultTimeout();
  }

  @Test
  void shouldReturnSingleResponseWithSimpleRequest() {
    StepVerifier.create(greetingService.one("hello")).expectNext("Echo:hello").verifyComplete();
  }

  @Test
  void shouldReturnSingleResponseWithSimpleLongDataRequest() {
    String data = new String(new char[500]);
    StepVerifier.create(greetingService.one(data)).expectNext("Echo:" + data).verifyComplete();
  }

  @Test
  void shouldReturnSingleResponseWithPojoRequest() {
    StepVerifier.create(greetingService.pojoOne(new GreetingRequest("hello")))
        .expectNextMatches(response -> "Echo:hello".equals(response.getText()))
        .verifyComplete();
  }

  @Test
  void shouldReturnListResponseWithPojoRequest() {
    StepVerifier.create(greetingService.pojoList(new GreetingRequest("hello")))
        .expectNextMatches(response -> "Echo:hello".equals(response.get(0).getText()))
        .verifyComplete();
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
        .verifyComplete();
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
        .verifyComplete();
  }

  @Test
  void shouldReturnErrorDataWhenServiceFails() {
    StepVerifier.create(greetingService.failingOne("hello"))
        .verifyErrorSatisfies(
            throwable -> assertInstanceOf(InternalServiceException.class, throwable));
  }

  @Test
  void shouldReturnErrorDataWhenRequestDataIsEmpty() {
    StepVerifier.create(greetingService.one(null))
        .verifyErrorSatisfies(
            ex -> {
              assertInstanceOf(BadRequestException.class, ex);
              assertThat(ex.getMessage(), startsWith("Wrong request"));
            });
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
        .expectNextMatches(
            resp -> {
              assertInstanceOf(EmptyGreetingResponse.class, resp);
              return true;
            })
        .verifyComplete();
  }

  @Test
  void shouldReturnOnEmptyMessageGreeting() {
    String qualifier = Qualifier.asString(GreetingService.NAMESPACE, "empty/wrappedPojo");
    ServiceMessage request =
        ServiceMessage.builder().qualifier(qualifier).data(new EmptyGreetingRequest()).build();
    StepVerifier.create(serviceCall.requestOne(request, true))
        .expectSubscription()
        .expectNextMatches(
            resp -> {
              assertInstanceOf(EmptyGreetingResponse.class, resp.data());
              return true;
            })
        .verifyComplete();
  }

  @Test
  void shouldReturnSomeExceptionOnFlux() {
    StepVerifier.create(errorService.manyError()).verifyError(SomeException.class);
  }

  @Test
  void shouldReturnSomeExceptionOnMono() {
    StepVerifier.create(errorService.oneError()).verifyError(SomeException.class);
  }

  @Test
  void shouldHeartbeat() {
    final var value = System.nanoTime();
    StepVerifier.create(serviceCall.api(HeartbeatService.class).ping(value))
        .assertNext(pongValue -> assertEquals(value, pongValue))
        .verifyComplete();
  }

  @Test
  void shouldWorkWithDynamicQualifier() {
    final var value = "12345";
    final var data = System.currentTimeMillis();
    final var request =
        ServiceMessage.builder().qualifier("greeting/hello/" + value).data(data).build();

    StepVerifier.create(serviceCall.requestOne(request, true).map(ServiceMessage::data))
        .assertNext(result -> assertEquals(value + "@" + data, result))
        .verifyComplete();
  }
}
