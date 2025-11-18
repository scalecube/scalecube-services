package io.scalecube.services.gateway.rest;

import static io.scalecube.services.api.ServiceMessage.HEADER_ERROR_TYPE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.collection.IsMapContaining.hasKey;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.scalecube.services.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.examples.GreetingServiceImpl;
import io.scalecube.services.exceptions.ServiceUnavailableException;
import io.scalecube.services.gateway.client.http.HttpGatewayClientTransport;
import io.scalecube.services.gateway.client.websocket.WebsocketGatewayClientTransport;
import io.scalecube.services.gateway.http.HttpGateway;
import io.scalecube.services.gateway.websocket.WebsocketGateway;
import io.scalecube.services.routing.StaticAddressRouter;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.time.Duration;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import reactor.test.StepVerifier;

public class RestGatewayTest {

  private static Microservices gateway;
  private static Microservices microservices;

  private static Address gatewayAddress;
  private static StaticAddressRouter router;

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
                .defaultLogger("gateway")
                .gateway(() -> HttpGateway.builder().id("HTTP").build())
                .gateway(() -> WebsocketGateway.builder().id("WS").build()));

    gatewayAddress = Address.from("localhost:" + gateway.gateway("HTTP").address().port());
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
                .services(ServiceInfo.fromServiceInstance(new RestServiceImpl()).build())
                .services(ServiceInfo.fromServiceInstance(new RoutingServiceImpl()).build()));
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

  private static ObjectMapper objectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    mapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
    mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    mapper.setVisibility(PropertyAccessor.ALL, Visibility.ANY);
    mapper.setSerializationInclusion(Include.NON_NULL);
    mapper.configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true);
    mapper.registerModule(new JavaTimeModule());
    return mapper;
  }

  @Nested
  @TestInstance(Lifecycle.PER_CLASS)
  class GatewayTests {

    private ServiceCall serviceCall;

    @BeforeAll
    void beforeAll() {
      serviceCall =
          new ServiceCall()
              .transport(HttpGatewayClientTransport.builder().address(gatewayAddress).build())
              .router(router);
    }

    @AfterAll
    void afterAll() {
      if (serviceCall != null) {
        serviceCall.close();
      }
    }

    @Test
    void testOptions() {
      final var param = "options" + System.currentTimeMillis();
      StepVerifier.create(
              serviceCall.requestOne(
                  ServiceMessage.builder()
                      .header("http.method", "OPTIONS")
                      .qualifier("v1/restService/options/" + param)
                      .build(),
                  SomeResponse.class))
          .assertNext(
              message -> {
                final var someResponse = message.<SomeResponse>data();
                assertNotNull(someResponse, "data");
                assertEquals(param, someResponse.name(), "someResponse.name");
              })
          .verifyComplete();
    }

    @Test
    void testGet() {
      final var param = "get" + System.currentTimeMillis();
      StepVerifier.create(
              serviceCall.requestOne(
                  ServiceMessage.builder()
                      .header("http.method", "GET")
                      .qualifier("v1/restService/get/" + param)
                      .build(),
                  SomeResponse.class))
          .assertNext(
              message -> {
                final var someResponse = message.<SomeResponse>data();
                assertNotNull(someResponse, "data");
                assertEquals(param, someResponse.name(), "someResponse.name");
              })
          .verifyComplete();
    }

    @Test
    void testHead() {
      final var param = "head123456";
      final var customHeader1 = "customHeader-" + System.currentTimeMillis();
      final var customHeader2 = "customHeader-" + System.currentTimeMillis();
      final var queryParam1 = "queryParam-" + System.currentTimeMillis();
      final var queryParam2 = "queryParam-" + System.currentTimeMillis();
      StepVerifier.create(
              serviceCall.requestOne(
                  ServiceMessage.builder()
                      .header("http.method", "HEAD")
                      .header("http.header.X-Custom-Header-1", customHeader1)
                      .header("http.header.X-Custom-Header-2", customHeader2)
                      .header("http.query.param1", queryParam1)
                      .header("http.query.param2", queryParam2)
                      .qualifier("v1/restService/head/" + param)
                      .build(),
                  SomeResponse.class))
          .assertNext(
              message -> {
                assertNull(message.data(), "data");
                assertNotNull(message.headers(), "headers");
                assertThat(message.headers(), not(hasKey(HEADER_ERROR_TYPE)));
              })
          .verifyComplete();
    }

    @Test
    void testPost() {
      final var name = "name" + System.currentTimeMillis();
      final var param = "post" + System.currentTimeMillis();
      StepVerifier.create(
              serviceCall.requestOne(
                  ServiceMessage.builder()
                      .header("http.method", "POST")
                      .qualifier("v1/restService/post/" + param)
                      .data(new SomeRequest().name(name))
                      .build(),
                  SomeResponse.class))
          .assertNext(
              message -> {
                final var someResponse = message.<SomeResponse>data();
                assertNotNull(someResponse, "data");
                assertEquals(name, someResponse.name(), "someResponse.name");
              })
          .verifyComplete();
    }

    @Test
    void testPut() {
      final var name = "name" + System.currentTimeMillis();
      final var param = "put" + System.currentTimeMillis();
      StepVerifier.create(
              serviceCall.requestOne(
                  ServiceMessage.builder()
                      .header("http.method", "PUT")
                      .qualifier("v1/restService/put/" + param)
                      .data(new SomeRequest().name(name))
                      .build(),
                  SomeResponse.class))
          .assertNext(
              message -> {
                final var someResponse = message.<SomeResponse>data();
                assertNotNull(someResponse, "data");
                assertEquals(name, someResponse.name(), "someResponse.name");
              })
          .verifyComplete();
    }

    @Test
    void testPatch() {
      final var name = "name" + System.currentTimeMillis();
      final var param = "patch" + System.currentTimeMillis();
      StepVerifier.create(
              serviceCall.requestOne(
                  ServiceMessage.builder()
                      .header("http.method", "PATCH")
                      .qualifier("v1/restService/patch/" + param)
                      .data(new SomeRequest().name(name))
                      .build(),
                  SomeResponse.class))
          .assertNext(
              message -> {
                final var someResponse = message.<SomeResponse>data();
                assertNotNull(someResponse, "data");
                assertEquals(name, someResponse.name(), "someResponse.name");
              })
          .verifyComplete();
    }

    @Test
    void testDelete() {
      final var name = "name" + System.currentTimeMillis();
      final var param = "delete" + System.currentTimeMillis();
      StepVerifier.create(
              serviceCall.requestOne(
                  ServiceMessage.builder()
                      .header("http.method", "DELETE")
                      .qualifier("v1/restService/delete/" + param)
                      .data(new SomeRequest().name(name))
                      .build(),
                  SomeResponse.class))
          .assertNext(
              message -> {
                final var someResponse = message.<SomeResponse>data();
                assertNotNull(someResponse, "data");
                assertEquals(param, someResponse.name(), "someResponse.name");
              })
          .verifyComplete();
    }

    @Test
    void testTrace() {
      final var param = "trace" + System.currentTimeMillis();
      StepVerifier.create(
              serviceCall.requestOne(
                  ServiceMessage.builder()
                      .header("http.method", "TRACE")
                      .qualifier("v1/restService/trace/" + param)
                      .build(),
                  SomeResponse.class))
          .assertNext(
              message -> {
                final var someResponse = message.<SomeResponse>data();
                assertNotNull(someResponse, "data");
                assertEquals(param, someResponse.name(), "someResponse.name");
              })
          .verifyComplete();
    }
  }

  @Nested
  @TestInstance(Lifecycle.PER_CLASS)
  class RoutingTests {

    private ServiceCall serviceCall;

    @BeforeAll
    void beforeAll() {
      serviceCall =
          new ServiceCall()
              .transport(HttpGatewayClientTransport.builder().address(gatewayAddress).build())
              .router(router);
    }

    @AfterAll
    void afterAll() {
      if (serviceCall != null) {
        serviceCall.close();
      }
    }

    @Test
    void testMatchByGetMethod() {
      final var param = "get" + System.currentTimeMillis();
      StepVerifier.create(
              serviceCall.requestOne(
                  ServiceMessage.builder()
                      .header("http.method", "GET")
                      .qualifier("v1/routingService/find/" + param)
                      .build(),
                  SomeResponse.class))
          .assertNext(
              message -> {
                final var someResponse = message.<SomeResponse>data();
                assertNotNull(someResponse, "data");
                assertEquals(param, someResponse.name(), "someResponse.name");
              })
          .verifyComplete();
    }

    @Test
    void testMatchByPostMethod() {
      final var name = "name" + System.currentTimeMillis();
      final var param = "post" + System.currentTimeMillis();
      StepVerifier.create(
              serviceCall.requestOne(
                  ServiceMessage.builder()
                      .header("http.method", "POST")
                      .qualifier("v1/routingService/update/" + param)
                      .data(new SomeRequest().name(name))
                      .build(),
                  SomeResponse.class))
          .assertNext(
              message -> {
                final var someResponse = message.<SomeResponse>data();
                assertNotNull(someResponse, "data");
                assertEquals(name, someResponse.name(), "someResponse.name");
              })
          .verifyComplete();
    }

    @Test
    void testNoMatchByRestMethod() {
      final var name = "name" + System.currentTimeMillis();
      final var param = "post" + System.currentTimeMillis();
      final var nonMatchedRestMethod = "PUT";
      StepVerifier.create(
              serviceCall.requestOne(
                  ServiceMessage.builder()
                      .header("http.method", nonMatchedRestMethod)
                      .qualifier("v1/routingService/update/" + param)
                      .data(new SomeRequest().name(name))
                      .build(),
                  SomeResponse.class))
          .verifyErrorSatisfies(
              ex -> {
                final var exception = (ServiceUnavailableException) ex;
                assertEquals(503, exception.errorCode(), "errorCode");
                assertThat(
                    exception.getMessage(),
                    Matchers.startsWith("No reachable member with such service"));
              });
    }

    @Test
    void testNoMatchWithoutRestMethod() {
      final var gatewayAddress = gateway.gateway("WS").address();
      final var router = StaticAddressRouter.forService(gatewayAddress, "app-service").build();
      final var clientTransport =
          WebsocketGatewayClientTransport.builder().address(gatewayAddress).build();

      try (final var serviceCall = new ServiceCall().router(router).transport(clientTransport)) {
        StepVerifier.create(serviceCall.api(RoutingService.class).update(new SomeRequest()))
            .expectSubscription()
            .expectErrorSatisfies(
                ex -> {
                  final var exception = (ServiceUnavailableException) ex;
                  assertEquals(503, exception.errorCode());
                  assertThat(
                      exception.getMessage(),
                      Matchers.startsWith("No reachable member with such service"));
                })
            .verify(Duration.ofSeconds(3));
      }
    }
  }
}
