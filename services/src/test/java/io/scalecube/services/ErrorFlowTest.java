package io.scalecube.services;

import static reactor.core.publisher.Mono.from;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.BadRequestException;
import io.scalecube.services.exceptions.ServiceUnavailableException;
import io.scalecube.services.exceptions.UnauthorizedException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.reactivestreams.Publisher;

import java.util.concurrent.atomic.AtomicInteger;

public class ErrorFlowTest {

  private static AtomicInteger port = new AtomicInteger(4000);
  private static Microservices provider;
  private static Microservices consumer;


  @BeforeClass
  public static void initNodes() {
    provider = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();
    consumer = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .seeds(provider.cluster().address())
        .build();
  }

  @AfterClass
  public static void shutdownNodes() {
    consumer.shutdown().block();
    provider.shutdown().block();
  }

  @Test(expected = BadRequestException.class)
  public void testCorruptedRequest() {
    Publisher<ServiceMessage> req = consumer
        .call().requestOne(TestRequests.GREETING_CORRUPTED_PAYLOAD_REQUEST);
    from(req).block();
  }

  @Test(expected = UnauthorizedException.class)
  public void testNotAuthorized() {
    Publisher<ServiceMessage> req = consumer
        .call().requestOne(TestRequests.GREETING_UNAUTHORIZED_REQUEST);
    from(req).block();
  }

  @Test(expected = BadRequestException.class)
  public void testNullRequestPayload() {
    Publisher<ServiceMessage> req = consumer
        .call().requestOne(TestRequests.GREETING_NULL_PAYLOAD);
    from(req).block();
  }

  @Test(expected = ServiceUnavailableException.class)
  public void testServiceUnavailable() {
    Publisher<ServiceMessage> req = consumer
        .call().requestOne(TestRequests.NOT_FOUND_REQ);
    from(req).block();
  }
}
