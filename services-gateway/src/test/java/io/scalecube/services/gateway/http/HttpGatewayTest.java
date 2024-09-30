package io.scalecube.services.gateway.http;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.scalecube.services.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.api.Qualifier;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.examples.EmptyGreetingRequest;
import io.scalecube.services.examples.EmptyGreetingResponse;
import io.scalecube.services.examples.GreetingRequest;
import io.scalecube.services.examples.GreetingService;
import io.scalecube.services.examples.GreetingServiceImpl;
import io.scalecube.services.exceptions.InternalServiceException;
import io.scalecube.services.gateway.BaseTest;
import io.scalecube.services.gateway.client.StaticAddressRouter;
import io.scalecube.services.gateway.client.http.HttpGatewayClientTransport;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

class HttpGatewayTest extends BaseTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(3);

  private static Microservices gateway;
  private static Address gatewayAddress;
  private static StaticAddressRouter router;
  private static Microservices services;

  private ServiceCall serviceCall;
  private GreetingService service;

  @BeforeAll
  static void beforeAll() {
    gateway =
        Microservices.builder()
            .discovery(
                serviceEndpoint ->
                    new ScalecubeServiceDiscovery()
                        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                        .options(opts -> opts.metadata(serviceEndpoint)))
            .transport(RSocketServiceTransport::new)
            .gateway(options -> new HttpGateway.Builder().options(options.id("HTTP")).build())
            .services(new GreetingServiceImpl())
            .startAwait();

    gatewayAddress = gateway.gateway("HTTP").address();
    router = new StaticAddressRouter(gatewayAddress);

    services =
        Microservices.builder()
            .discovery(
                serviceEndpoint ->
                    new ScalecubeServiceDiscovery()
                        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                        .options(opts -> opts.metadata(serviceEndpoint)))
            .transport(RSocketServiceTransport::new)
            .services(new GreetingServiceImpl())
            .startAwait();
  }

  @BeforeEach
  void beforeEach() {
    serviceCall =
        new ServiceCall()
            .router(router)
            .transport(new HttpGatewayClientTransport.Builder().address(gatewayAddress).build());
    service = serviceCall.api(GreetingService.class);
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
    if (services != null) {
      services.close();
    }
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
  void shouldReturnNoContentWhenResponseIsEmpty() {
    StepVerifier.create(service.emptyOne("hello")).expectComplete().verify(TIMEOUT);
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
    StepVerifier.create(serviceCall.requestOne(request, EmptyGreetingResponse.class))
        .expectSubscription()
        .expectNextMatches(resp -> resp.data() instanceof EmptyGreetingResponse)
        .thenCancel()
        .verify();
  }
}
