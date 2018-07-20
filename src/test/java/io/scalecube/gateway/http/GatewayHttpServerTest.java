package io.scalecube.gateway.http;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.scalecube.gateway.MicroservicesExtension;
import io.scalecube.services.Microservices;

import reactor.core.publisher.Mono;
import reactor.ipc.netty.http.client.HttpClient;
import reactor.ipc.netty.http.client.HttpClientException;
import reactor.ipc.netty.http.client.HttpClientResponse;
import reactor.test.StepVerifier;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.InetSocketAddress;

public class GatewayHttpServerTest {

  @RegisterExtension
  public static MicroservicesExtension microservicesResource = new MicroservicesExtension();
  @RegisterExtension
  public static GatewayHttpExtension gatewayHttpResource = new GatewayHttpExtension();

  private static HttpClient client;

  @BeforeAll
  public static void setUp() {
    final Microservices gatewayMicroservice = microservicesResource.startGateway().getGateway();

    gatewayHttpResource.startGateway(gatewayMicroservice);
    microservicesResource.startServices(gatewayMicroservice.cluster().address());

    client = gatewayHttpResource.client();
  }

  @Test
  public void shouldReturnOKWhenRequestIsString() {
    final String message = "\"hello\"";

    final Mono<HttpClientResponse> post = client
        .post(generateURL("/greeting/one"), request -> request.sendString(Mono.just(message)));

    StepVerifier.create(post)
        .assertNext(response -> assertEquals(OK, response.status()))
        .verifyComplete();
  }

  @Test
  public void shouldReturnOKWhenRequestIsObject() {
    final String message = "{\"text\":\"hello\"}";

    final Mono<HttpClientResponse> post = client
        .post(generateURL("/greeting/pojo/one"), request -> request.sendString(Mono.just(message)));

    StepVerifier.create(post)
        .assertNext(response -> assertEquals(OK, response.status()))
        .verifyComplete();
  }

  @Test
  public void shouldReturnNoContentWhenResponseIsEmpty() {
    final String message = "\"hello\"";

    final Mono<HttpClientResponse> post = client
        .post(generateURL("/greeting/empty/one"), request -> request.sendString(Mono.just(message)));

    StepVerifier.create(post)
        .assertNext(response -> assertEquals(NO_CONTENT, response.status()))
        .verifyComplete();
  }

  @Test
  public void shouldReturnServiceUnavailableWhenQualifierIsWrong() {
    final String message = "\"hello\"";

    final Mono<HttpClientResponse> post = client
        .post(generateURL("/greeting/zzz"), request -> request.sendString(Mono.just(message)));

    StepVerifier.create(post)
        .verifyErrorSatisfies(throwable -> {
          assertEquals(HttpClientException.class, throwable.getClass());
          assertEquals(SERVICE_UNAVAILABLE, ((HttpClientException) throwable).status());
        });
  }

  @Test
  public void shouldReturnInternalServerErrorWhenRequestIsInvalid() {
    final String message = "@@@";

    final Mono<HttpClientResponse> post = client
        .post(generateURL("/greeting/one"), request -> request.sendString(Mono.just(message)));

    StepVerifier.create(post)
        .verifyErrorSatisfies(throwable -> {
          assertEquals(HttpClientException.class, throwable.getClass());
          assertEquals(INTERNAL_SERVER_ERROR, ((HttpClientException) throwable).status());
        });
  }

  @Test
  public void shouldReturnInternalServerErrorWhenServiceFails() {
    final String message = "\"hello\"";

    final Mono<HttpClientResponse> post = client
        .post(generateURL("/greeting/failing/one"), request -> request.sendString(Mono.just(message)));

    StepVerifier.create(post)
        .verifyErrorSatisfies(throwable -> {
          assertEquals(HttpClientException.class, throwable.getClass());
          assertEquals(INTERNAL_SERVER_ERROR, ((HttpClientException) throwable).status());
        });
  }

  @Test
  public void shouldReturnInternalServerErrorWhenTimeoutReached() {
    final String message = "\"hello\"";

    final Mono<HttpClientResponse> post = client
        .post(generateURL("/greeting/never/one"), request -> request.sendString(Mono.just(message)));

    StepVerifier.create(post)
        .verifyErrorSatisfies(throwable -> {
          assertEquals(HttpClientException.class, throwable.getClass());
          assertEquals(INTERNAL_SERVER_ERROR, ((HttpClientException) throwable).status());
        });
  }

  private String generateURL(String qualifier) {
    final InetSocketAddress address = gatewayHttpResource.gateway().address();
    return "http://" + address.getHostName() + ":" + address.getPort() + qualifier;
  }

}
