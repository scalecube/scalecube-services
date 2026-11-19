package io.scalecube.services.gateway.websocket;

import static io.scalecube.services.gateway.GatewayErrorMapperImpl.ERROR_MAPPER;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.services.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.gateway.EchoService;
import io.scalecube.services.gateway.EchoServiceImpl;
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;

/** Malformed sid, sig or rlimit must get a bad-request error frame back, not silence. */
class WebsocketGatewayBadRequestTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(3);
  private static final String QUALIFIER = EchoService.class.getName() + "/echo";

  private static Microservices gateway;

  @BeforeAll
  static void beforeAll() {
    gateway =
        Microservices.start(
            new Context()
                .gateway(() -> WebsocketGateway.builder().id("WS").build())
                .services(
                    ServiceInfo.fromServiceInstance(new EchoServiceImpl())
                        .errorMapper(ERROR_MAPPER)
                        .build()));
  }

  @AfterAll
  static void afterAll() {
    if (gateway != null) {
      gateway.close();
    }
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "\"sid\":\"abc\"            | sid is invalid",
        "\"sid\":1,\"sig\":9        | sig is invalid",
        "\"sid\":1,\"sig\":\"x\"    | sig is invalid",
        "\"sid\":1,\"rlimit\":0     | rlimit is invalid",
        "\"sid\":1,\"rlimit\":\"x\" | rlimit is invalid",
      })
  void malformedHeaderGetsBadRequestError(String headers, String expectedMessage) {
    final String response =
        sendAndReceive("{\"q\":\"" + QUALIFIER + "\"," + headers + ",\"d\":\"hello\"}");

    assertTrue(response.contains("\"sig\":2"), "error signal expected: " + response);
    assertTrue(response.contains("400"), "bad request expected: " + response);
    assertTrue(response.contains(expectedMessage), "error message expected: " + response);
  }

  private static String sendAndReceive(String frame) {
    final Address address = gateway.gateway("WS").address();
    return HttpClient.create()
        .websocket()
        .uri("ws://" + address.host() + ":" + address.port() + "/")
        .handle(
            (in, out) ->
                out.sendString(Mono.just(frame))
                    .then()
                    .thenMany(in.receive().asString().next()))
        .next()
        .block(TIMEOUT);
  }
}
