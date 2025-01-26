package io.scalecube.services.gateway.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.scalecube.services.Microservices;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.api.ErrorData;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.examples.GreetingServiceImpl;
import io.scalecube.services.exceptions.ServiceUnavailableException;
import io.scalecube.services.gateway.client.StaticAddressRouter;
import io.scalecube.services.gateway.client.websocket.WebsocketGatewayClientTransport.Builder;
import io.scalecube.services.gateway.http.HttpGateway;
import io.scalecube.services.gateway.websocket.WebsocketGateway;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

public class RestGatewayTest {

  private static Microservices gateway;
  private static Microservices microservices;
  private static HttpClient httpClient;

  private final ObjectMapper objectMapper = objectMapper();
  private static String httpGatewayAddress;

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
                .gateway(() -> new HttpGateway.Builder().id("HTTP").build())
                .gateway(() -> new WebsocketGateway.Builder().id("WS").build()));

    httpGatewayAddress = "http://localhost:" + gateway.gateway("HTTP").address().port();

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

    httpClient = HttpClient.newHttpClient();
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
    mapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    mapper.configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true);
    mapper.registerModule(new JavaTimeModule());
    return mapper;
  }

  @Nested
  class GatewayTests {

    @Test
    void testOptions() throws Exception {
      final var fooParam = "options" + System.currentTimeMillis();
      final var httpRequest =
          HttpRequest.newBuilder(
                  new URI(httpGatewayAddress + "/v1/restService/options/" + fooParam))
              .method("OPTIONS", BodyPublishers.noBody())
              .build();
      final var httpResponse = httpClient.send(httpRequest, BodyHandlers.ofString());
      assertEquals(HttpResponseStatus.OK.code(), httpResponse.statusCode(), "statusCode");
      assertEquals(
          fooParam,
          objectMapper.readValue(httpResponse.body(), SomeResponse.class).name(),
          "response");
    }

    @Test
    void testGet() throws Exception {
      final var fooParam = "get" + System.currentTimeMillis();
      final var httpRequest =
          HttpRequest.newBuilder(new URI(httpGatewayAddress + "/v1/restService/get/" + fooParam))
              .method("GET", BodyPublishers.noBody())
              .build();
      final var httpResponse = httpClient.send(httpRequest, BodyHandlers.ofString());
      assertEquals(HttpResponseStatus.OK.code(), httpResponse.statusCode(), "statusCode");
      assertEquals(
          fooParam,
          objectMapper.readValue(httpResponse.body(), SomeResponse.class).name(),
          "response");
    }

    @Test
    void testHead() throws Exception {
      final var fooParam = "head" + System.currentTimeMillis();
      final var httpRequest =
          HttpRequest.newBuilder(new URI(httpGatewayAddress + "/v1/restService/head/" + fooParam))
              .method("HEAD", BodyPublishers.noBody())
              .build();
      final var httpResponse = httpClient.send(httpRequest, BodyHandlers.ofString());
      assertEquals(HttpResponseStatus.OK.code(), httpResponse.statusCode(), "statusCode");
      assertEquals("", httpResponse.body(), "response");
    }

    @Test
    void testPost() throws Exception {
      final var name = "name" + System.currentTimeMillis();
      final var fooParam = "post" + System.currentTimeMillis();
      final var httpRequest =
          HttpRequest.newBuilder(new URI(httpGatewayAddress + "/v1/restService/post/" + fooParam))
              .method(
                  "POST",
                  BodyPublishers.ofString(
                      objectMapper.writeValueAsString(new SomeRequest().name(name))))
              .build();
      final var httpResponse = httpClient.send(httpRequest, BodyHandlers.ofString());
      assertEquals(HttpResponseStatus.OK.code(), httpResponse.statusCode(), "statusCode");
      assertEquals(
          name, objectMapper.readValue(httpResponse.body(), SomeResponse.class).name(), "response");
    }

    @Test
    void testPut() throws Exception {
      final var name = "name" + System.currentTimeMillis();
      final var fooParam = "put" + System.currentTimeMillis();
      final var httpRequest =
          HttpRequest.newBuilder(new URI(httpGatewayAddress + "/v1/restService/put/" + fooParam))
              .method(
                  "PUT",
                  BodyPublishers.ofString(
                      objectMapper.writeValueAsString(new SomeRequest().name(name))))
              .build();
      final var httpResponse = httpClient.send(httpRequest, BodyHandlers.ofString());
      assertEquals(HttpResponseStatus.OK.code(), httpResponse.statusCode(), "statusCode");
      assertEquals(
          name, objectMapper.readValue(httpResponse.body(), SomeResponse.class).name(), "response");
    }

    @Test
    void testPatch() throws Exception {
      final var name = "name" + System.currentTimeMillis();
      final var fooParam = "patch" + System.currentTimeMillis();
      final var httpRequest =
          HttpRequest.newBuilder(new URI(httpGatewayAddress + "/v1/restService/patch/" + fooParam))
              .method(
                  "PATCH",
                  BodyPublishers.ofString(
                      objectMapper.writeValueAsString(new SomeRequest().name(name))))
              .build();
      final var httpResponse = httpClient.send(httpRequest, BodyHandlers.ofString());
      assertEquals(HttpResponseStatus.OK.code(), httpResponse.statusCode(), "statusCode");
      assertEquals(
          name, objectMapper.readValue(httpResponse.body(), SomeResponse.class).name(), "response");
    }

    @Test
    void testDelete() throws Exception {
      final var name = "name" + System.currentTimeMillis();
      final var fooParam = "delete" + System.currentTimeMillis();
      final var httpRequest =
          HttpRequest.newBuilder(new URI(httpGatewayAddress + "/v1/restService/delete/" + fooParam))
              .method(
                  "DELETE",
                  BodyPublishers.ofString(
                      objectMapper.writeValueAsString(new SomeRequest().name(name))))
              .build();
      final var httpResponse = httpClient.send(httpRequest, BodyHandlers.ofString());
      assertEquals(HttpResponseStatus.OK.code(), httpResponse.statusCode(), "statusCode");
      assertEquals(
          name, objectMapper.readValue(httpResponse.body(), SomeResponse.class).name(), "response");
    }

    @Test
    void testTrace() throws Exception {
      final var fooParam = "trace" + System.currentTimeMillis();
      final var httpRequest =
          HttpRequest.newBuilder(new URI(httpGatewayAddress + "/v1/restService/trace/" + fooParam))
              .method("TRACE", BodyPublishers.noBody())
              .build();
      final var httpResponse = httpClient.send(httpRequest, BodyHandlers.ofString());
      assertEquals(HttpResponseStatus.OK.code(), httpResponse.statusCode(), "statusCode");
      assertEquals(
          fooParam,
          objectMapper.readValue(httpResponse.body(), SomeResponse.class).name(),
          "response");
    }
  }

  @Nested
  class RoutingTests {

    @Test
    void testMatchByGetMethod() throws Exception {
      final var fooParam = "get" + System.currentTimeMillis();
      final var httpRequest =
          HttpRequest.newBuilder(
                  new URI(httpGatewayAddress + "/v1/routingService/find/" + fooParam))
              .method("GET", BodyPublishers.noBody())
              .build();
      final var httpResponse = httpClient.send(httpRequest, BodyHandlers.ofString());
      assertEquals(HttpResponseStatus.OK.code(), httpResponse.statusCode(), "statusCode");
      assertEquals(
          fooParam,
          objectMapper.readValue(httpResponse.body(), SomeResponse.class).name(),
          "response");
    }

    @Test
    void testMatchByPostMethod() throws Exception {
      final var name = "name" + System.currentTimeMillis();
      final var fooParam = "post" + System.currentTimeMillis();
      final var httpRequest =
          HttpRequest.newBuilder(
                  new URI(httpGatewayAddress + "/v1/routingService/update/" + fooParam))
              .method(
                  "POST",
                  BodyPublishers.ofString(
                      objectMapper.writeValueAsString(new SomeRequest().name(name))))
              .build();
      final var httpResponse = httpClient.send(httpRequest, BodyHandlers.ofString());
      assertEquals(HttpResponseStatus.OK.code(), httpResponse.statusCode(), "statusCode");
      assertEquals(
          name, objectMapper.readValue(httpResponse.body(), SomeResponse.class).name(), "response");
    }

    @Test
    void testNoMatchByRestMethod() throws Exception {
      final var name = "name" + System.currentTimeMillis();
      final var fooParam = "post" + System.currentTimeMillis();
      final var nonMatchedRestMethod = "PUT";
      final var httpRequest =
          HttpRequest.newBuilder(
                  new URI(httpGatewayAddress + "/v1/routingService/update/" + fooParam))
              .method(
                  nonMatchedRestMethod,
                  BodyPublishers.ofString(
                      objectMapper.writeValueAsString(new SomeRequest().name(name))))
              .build();
      final var httpResponse = httpClient.send(httpRequest, BodyHandlers.ofString());
      assertEquals(
          HttpResponseStatus.SERVICE_UNAVAILABLE.code(), httpResponse.statusCode(), "statusCode");
      assertTrue(
          objectMapper
              .readValue(httpResponse.body(), ErrorData.class)
              .getErrorMessage()
              .startsWith("No reachable member with such service"));
    }

    @Test
    void testNoMatchWithoutRestMethod() {
      final var gatewayAddress = gateway.gateway("WS").address();
      final var router = new StaticAddressRouter(gatewayAddress);
      final var clientTransport = new Builder().address(gatewayAddress).build();

      try (final var serviceCall = new ServiceCall().router(router).transport(clientTransport)) {
        StepVerifier.create(serviceCall.api(RoutingService.class).update(new SomeRequest()))
            .expectSubscription()
            .expectErrorSatisfies(
                ex -> {
                  final var exception = (ServiceUnavailableException) ex;
                  final var errorMessage = exception.getMessage();
                  assertEquals(503, exception.errorCode());
                  assertTrue(errorMessage.startsWith("No reachable member with such service"));
                })
            .verify(Duration.ofSeconds(3));
      }
    }
  }
}
