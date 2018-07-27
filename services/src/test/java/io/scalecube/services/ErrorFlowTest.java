package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static reactor.core.publisher.Mono.from;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.BadRequestException;
import io.scalecube.services.exceptions.InternalServiceException;
import io.scalecube.services.exceptions.ServiceUnavailableException;
import io.scalecube.services.exceptions.UnauthorizedException;
import io.scalecube.services.sut.GreetingResponse;
import io.scalecube.services.sut.GreetingServiceImpl;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import reactor.test.StepVerifier;

import java.util.concurrent.atomic.AtomicInteger;

public class ErrorFlowTest {

  private static AtomicInteger port = new AtomicInteger(4000);
  private static Microservices provider;
  private static Microservices consumer;


  @BeforeAll
  public static void initNodes() {
    provider = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .startAwait();
    consumer = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .seeds(provider.discovery().address())
        .startAwait();
  }

  @AfterAll
  public static void shutdownNodes() {
    consumer.shutdown().block();
    provider.shutdown().block();
  }

  @Test
  public void testCorruptedRequest() {
    Publisher<ServiceMessage> req = consumer
        .call().create().requestOne(TestRequests.GREETING_CORRUPTED_PAYLOAD_REQUEST, GreetingResponse.class);
    assertThrows(InternalServiceException.class, () -> from(req).block());
  }

  @Test
  public void testNotAuthorized() {
    Publisher<ServiceMessage> req = consumer
        .call().create().requestOne(TestRequests.GREETING_UNAUTHORIZED_REQUEST, GreetingResponse.class);
    assertThrows(UnauthorizedException.class, () -> from(req).block());
  }

  @Test
  public void testNullRequestPayload() {
    Publisher<ServiceMessage> req = consumer
        .call().create().requestOne(TestRequests.GREETING_NULL_PAYLOAD, GreetingResponse.class);
    assertThrows(BadRequestException.class, () -> from(req).block());
  }

  @Test
  public void testServiceUnavailable() {
    StepVerifier.create(consumer.call().create().requestOne(TestRequests.NOT_FOUND_REQ))
        .expectError(ServiceUnavailableException.class)
        .verify();
  }
}
