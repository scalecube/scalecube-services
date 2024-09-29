package io.scalecube.services.gateway.http;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.scalecube.services.Microservices;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.examples.GreetingService;
import io.scalecube.services.examples.GreetingServiceImpl;
import io.scalecube.services.gateway.BaseTest;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.netty.ByteBufFlux;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClientResponse;
import reactor.netty.resources.ConnectionProvider;

public class CorsTest extends BaseTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(3);
  public static final int HTTP_PORT = 8999;

  private Microservices gateway;

  private final Microservices.Builder gatewayBuilder =
      Microservices.builder()
          .discovery(
              serviceEndpoint ->
                  new ScalecubeServiceDiscovery()
                      .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                      .options(opts -> opts.metadata(serviceEndpoint)))
          .transport(RSocketServiceTransport::new)
          .services(new GreetingServiceImpl());
  private HttpClient client;

  @BeforeEach
  void beforeEach() {
    client = HttpClient.create(ConnectionProvider.newConnection()).port(HTTP_PORT).wiretap(true);
  }

  @AfterEach
  void afterEach() {
    if (gateway != null) {
      gateway.shutdown().block();
    }
  }

  @Test
  void testCrossOriginRequest() {
    gateway =
        gatewayBuilder
            .gateway(
                opts ->
                    new HttpGateway.Builder()
                        .options(opts.id("http").port(HTTP_PORT))
                        .corsEnabled(true)
                        .corsConfigBuilder(
                            builder ->
                                builder.allowedRequestHeaders("Content-Type", "X-Correlation-ID"))
                        .build())
            .start()
            .block(TIMEOUT);

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
    assertEquals("POST", responseHeaders.get("Access-Control-Allow-Methods"));
    assertThat(responseHeaders.get("Access-Control-Allow-Headers"), containsString("Content-Type"));
    assertThat(
        responseHeaders.get("Access-Control-Allow-Headers"), containsString("X-Correlation-ID"));

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

  @Test
  void testOptionRequestCorsDisabled() {
    gateway =
        gatewayBuilder
            .gateway(
                opts -> new HttpGateway.Builder().options(opts.id("http").port(HTTP_PORT)).build())
            .start()
            .block(TIMEOUT);

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

    assertEquals(HttpResponseStatus.METHOD_NOT_ALLOWED, response.status());
    assertNull(responseHeaders.get("Access-Control-Allow-Origin"));
    assertNull(responseHeaders.get("Access-Control-Allow-Methods"));
    assertNull(responseHeaders.get("Access-Control-Allow-Headers"));
  }
}
