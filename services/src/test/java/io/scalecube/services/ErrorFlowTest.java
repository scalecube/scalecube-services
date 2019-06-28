package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static reactor.core.publisher.Mono.from;

import io.scalecube.net.Address;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.exceptions.BadRequestException;
import io.scalecube.services.exceptions.InternalServiceException;
import io.scalecube.services.exceptions.ServiceUnavailableException;
import io.scalecube.services.exceptions.UnauthorizedException;
import io.scalecube.services.sut.GreetingResponse;
import io.scalecube.services.sut.GreetingServiceImpl;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.test.StepVerifier;

public class ErrorFlowTest {

  private static AtomicInteger port = new AtomicInteger(4000);
  private static Microservices provider;
  private static Microservices consumer;

  /**
   * init.
   */
  @BeforeAll
  public static void initNodes() {
    provider =
        Microservices.builder()
            .discovery(
                serviceEndpoint ->
                    new ScalecubeServiceDiscovery(serviceEndpoint)
                        .options(opts -> opts.port(port.incrementAndGet())))
            .transport(opts -> opts.serviceTransport(RSocketServiceTransport::new))
            .services(new GreetingServiceImpl())
            .startAwait();

    Address seedAddress = provider.discovery().address();

    consumer =
        Microservices.builder()
            .discovery(
                serviceEndpoint ->
                    new ScalecubeServiceDiscovery(serviceEndpoint)
                        .options(
                            opts -> opts.seedMembers(seedAddress).port(port.incrementAndGet())))
            .transport(opts -> opts.serviceTransport(RSocketServiceTransport::new))
            .startAwait();
  }

  @AfterAll
  public static void shutdownNodes() {
    consumer.shutdown().block();
    provider.shutdown().block();
  }

  @Test
  public void testCorruptedRequest() {
    Publisher<ServiceMessage> req =
        consumer
            .call()
            .requestOne(TestRequests.GREETING_CORRUPTED_PAYLOAD_REQUEST, GreetingResponse.class);
    assertThrows(InternalServiceException.class, () -> from(req).block());
  }

  @Test
  public void testNotAuthorized() {
    Publisher<ServiceMessage> req =
        consumer
            .call()
            .requestOne(TestRequests.GREETING_UNAUTHORIZED_REQUEST, GreetingResponse.class);
    assertThrows(UnauthorizedException.class, () -> from(req).block());
  }

  @Test
  public void testNullRequestPayload() {
    Publisher<ServiceMessage> req =
        consumer.call().requestOne(TestRequests.GREETING_NULL_PAYLOAD, GreetingResponse.class);
    assertThrows(BadRequestException.class, () -> from(req).block());
  }

  @Test
  public void testServiceUnavailable() {
    StepVerifier.create(consumer.call().requestOne(TestRequests.NOT_FOUND_REQ))
        .expectError(ServiceUnavailableException.class)
        .verify();
  }
}
