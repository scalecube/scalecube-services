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
import io.scalecube.services.inject.ScalecubeServiceFactory;
import io.scalecube.services.sut.GreetingResponse;
import io.scalecube.services.sut.GreetingServiceImpl;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.test.StepVerifier;

public class ErrorFlowTest extends BaseTest {

  private static AtomicInteger port = new AtomicInteger(4000);
  private static Microservices provider;
  private static Microservices consumer;

  @BeforeAll
  public static void initNodes() {
    provider =
        Microservices.builder()
            .discovery(
                endpoint ->
                    new ScalecubeServiceDiscovery(endpoint)
                        .transport(cfg -> cfg.port(port.incrementAndGet())))
            .transport(RSocketServiceTransport::new)
            .serviceFactory(ScalecubeServiceFactory.from(new GreetingServiceImpl()))
            .startAwait();

    final Address seedAddress = provider.discovery().address();

    consumer =
        Microservices.builder()
            .discovery(
                endpoint ->
                    new ScalecubeServiceDiscovery(endpoint)
                        .membership(cfg -> cfg.seedMembers(seedAddress))
                        .transport(cfg -> cfg.port(port.incrementAndGet())))
            .transport(RSocketServiceTransport::new)
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
