package io.scalecube.services.gateway.http;

import static io.scalecube.services.gateway.GatewayErrorMapperImpl.ERROR_MAPPER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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
import io.scalecube.services.examples.GreetingService;
import io.scalecube.services.examples.GreetingServiceImpl;
import io.scalecube.services.exceptions.InternalServiceException;
import io.scalecube.services.gateway.ErrorService;
import io.scalecube.services.gateway.ErrorServiceImpl;
import io.scalecube.services.gateway.SomeException;
import io.scalecube.services.gateway.client.http.HttpGatewayClientTransport;
import io.scalecube.services.gateway.sut.typed.Circle;
import io.scalecube.services.gateway.sut.typed.Rectangle;
import io.scalecube.services.gateway.sut.typed.Square;
import io.scalecube.services.gateway.sut.typed.StartOfDayEvent;
import io.scalecube.services.routing.StaticAddressRouter;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

class HttpGatewayTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(3);

  private static Microservices gateway;
  private static Address gatewayAddress;
  private static StaticAddressRouter router;
  private static Microservices microservices;

  private ServiceCall serviceCall;
  private GreetingService greetingService;
  private TypedGreetingService typedGreetingService;
  private ErrorService errorService;

  @BeforeAll
  static void beforeAll() {
    gateway =
        Microservices.start(
            new Context()
                .discovery(
                    serviceEndpoint ->
                        new ScalecubeServiceDiscovery()
                            .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                            .options(opts -> opts.metadata(serviceEndpoint)))
                .transport(RSocketServiceTransport::new)
                .gateway(() -> HttpGateway.builder().id("HTTP").errorMapper(ERROR_MAPPER).build()));

    gatewayAddress = gateway.gateway("HTTP").address();
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
                .services(new GreetingServiceImpl(), new TypedGreetingServiceImpl())
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
            .transport(HttpGatewayClientTransport.builder().address(gatewayAddress).build());
    greetingService = serviceCall.api(GreetingService.class);
    typedGreetingService = serviceCall.api(TypedGreetingService.class);
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
  void shouldReturnNoContentWhenResponseIsEmpty() {
    StepVerifier.create(greetingService.emptyOne("hello")).expectComplete().verify(TIMEOUT);
  }

  @Test
  void shouldReturnInternalServerErrorWhenServiceFails() {
    StepVerifier.create(greetingService.failingOne("hello"))
        .expectErrorSatisfies(
            throwable -> {
              assertEquals(InternalServiceException.class, throwable.getClass());
              assertEquals("hello", throwable.getMessage());
            })
        .verify(TIMEOUT);
  }

  @Test
  void shouldSuccessfullyReuseServiceProxy() {
    StepVerifier.create(greetingService.one("hello"))
        .expectNext("Echo:hello")
        .expectComplete()
        .verify(TIMEOUT);

    StepVerifier.create(greetingService.one("hello"))
        .expectNext("Echo:hello")
        .expectComplete()
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

  @Disabled
  @Test
  void shouldReturnSomeException() {
    StepVerifier.create(errorService.oneError()).expectError(SomeException.class).verify(TIMEOUT);
  }

  @Test
  void shouldWorkWithDynamicQualifier() {
    final var value = "12345";
    final var data = System.currentTimeMillis();
    final var request =
        ServiceMessage.builder().qualifier("greeting/hello/" + value).data(data).build();

    StepVerifier.create(serviceCall.requestOne(request, String.class).map(ServiceMessage::data))
        .assertNext(result -> assertEquals(value + "@" + data, result))
        .verifyComplete();
  }

  @Test
  public void shouldReturnPolymorph() {
    StepVerifier.create(typedGreetingService.helloPolymorph())
        .assertNext(shape -> assertEquals(1.0, ((Circle) shape).radius()))
        .thenCancel()
        .verify();
  }

  @Test
  public void shouldReturnListPolymorph() {
    StepVerifier.create(typedGreetingService.helloListPolymorph())
        .assertNext(
            shapes -> {
              assertEquals(1.0, ((Circle) shapes.get(0)).radius());
              assertEquals(1.0, ((Rectangle) shapes.get(1)).height());
              assertEquals(1.0, ((Rectangle) shapes.get(1)).width());
              assertEquals(1.0, ((Square) shapes.get(2)).side());
            })
        .thenCancel()
        .verify();
  }

  @Test
  public void shouldReturnMultitype() {
    StepVerifier.create(typedGreetingService.helloMultitype())
        .assertNext(
            event -> {
              final var sodEvent = (StartOfDayEvent) event;
              assertEquals(1, sodEvent.timestamp());
              assertEquals(1, sodEvent.trackingNumber());
              assertEquals(1, sodEvent.eventId());
              assertNotNull(sodEvent.sodTime());
            })
        .thenCancel()
        .verify();
  }

  @Test
  public void shouldReturnWildcardMultitype() {
    StepVerifier.create(typedGreetingService.helloWildcardMultitype())
        .assertNext(
            event -> {
              final var sodEvent = (StartOfDayEvent) event;
              assertEquals(1, sodEvent.timestamp());
              assertEquals(1, sodEvent.trackingNumber());
              assertEquals(1, sodEvent.eventId());
              assertNotNull(sodEvent.sodTime());
            })
        .thenCancel()
        .verify();
  }
}
