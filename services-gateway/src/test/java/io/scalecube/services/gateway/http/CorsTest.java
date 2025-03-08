package io.scalecube.services.gateway.http;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.scalecube.services.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.examples.GreetingService;
import io.scalecube.services.examples.GreetingServiceImpl;
import java.time.Duration;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.netty.ByteBufFlux;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClientResponse;
import reactor.netty.resources.ConnectionProvider;

public class CorsTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(3);

  private Microservices gateway;
  private ConnectionProvider connectionProvider;

  @BeforeEach
  void setUp() {
    connectionProvider = ConnectionProvider.newConnection();
  }

  @AfterEach
  void afterEach() {
    if (gateway != null) {
      gateway.close();
    }
    if (connectionProvider != null) {
      connectionProvider.dispose();
    }
  }

  @Test
  void testCrossOriginRequest() {
    gateway =
        Microservices.start(
            new Context()
                .gateway(
                    () ->
                        HttpGateway.builder()
                            .id("http")
                            .corsEnabled(true)
                            .corsConfigBuilder(
                                builder ->
                                    builder.allowedRequestHeaders(
                                        "Content-Type", "X-Correlation-ID"))
                            .build())
                .services(new GreetingServiceImpl()));

    final HttpClient client = newClient(gateway.gateway("http").address());

    HttpClientResponse response =
        client
            .headers(
                headers ->
                    headers
                        .add("Origin", "test.com")
                        .add("Access-Control-Request-Method", "POST")
                        .add("Access-Control-Request-Headers", "Content-Type,X-Correlation-ID"))
            .options()
            .response()
            .block(TIMEOUT);

    HttpHeaders responseHeaders = response.responseHeaders();

    assertEquals(HttpResponseStatus.OK, response.status());
    assertEquals("*", responseHeaders.get("Access-Control-Allow-Origin"));
    assertThat(responseHeaders.get("Access-Control-Allow-Headers"), containsString("Content-Type"));
    assertThat(
        responseHeaders.get("Access-Control-Allow-Headers"), containsString("X-Correlation-ID"));
    final var allowedMethodsHeader = responseHeaders.get("Access-Control-Allow-Methods");
    assertEquals(
        HttpGateway.SUPPORTED_METHODS.stream().map(HttpMethod::name).collect(Collectors.toSet()),
        Arrays.stream(allowedMethodsHeader.split(",")).collect(Collectors.toSet()),
        "allowedMethods");

    response =
        client
            .headers(
                headers ->
                    headers
                        .add("Origin", "test.com")
                        .add("X-Correlation-ID", "xxxxxx")
                        .add("Content-Type", "application/json"))
            .post()
            .uri("/" + GreetingService.NAMESPACE + "/one")
            .send(ByteBufFlux.fromString(Mono.just("\"Hello\"")))
            .response()
            .block(TIMEOUT);

    responseHeaders = response.responseHeaders();

    assertEquals(HttpResponseStatus.OK, response.status());
    assertEquals("*", responseHeaders.get("Access-Control-Allow-Origin"));
  }

  private HttpClient newClient(final Address address) {
    return HttpClient.create(connectionProvider).port(address.port());
  }

  @Test
  void testOptionRequestCorsDisabled() {
    gateway =
        Microservices.start(
            new Context()
                .gateway(() -> HttpGateway.builder().id("http").build())
                .services(new GreetingServiceImpl()));

    final HttpClient client = newClient(gateway.gateway("http").address());

    HttpClientResponse response =
        client
            .headers(
                headers ->
                    headers
                        .add("Origin", "test.com")
                        .add("Access-Control-Request-Method", "POST")
                        .add("Access-Control-Request-Headers", "Content-Type,X-Correlation-ID"))
            .options()
            .response()
            .block(TIMEOUT);

    HttpHeaders responseHeaders = response.responseHeaders();

    assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, response.status());
    assertNull(responseHeaders.get("Access-Control-Allow-Origin"));
    assertNull(responseHeaders.get("Access-Control-Allow-Methods"));
    assertNull(responseHeaders.get("Access-Control-Allow-Headers"));
  }
}
