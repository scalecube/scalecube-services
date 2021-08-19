package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static reactor.core.publisher.Mono.from;

import io.scalecube.net.Address;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.exceptions.BadRequestException;
import io.scalecube.services.exceptions.ForbiddenException;
import io.scalecube.services.exceptions.InternalServiceException;
import io.scalecube.services.exceptions.ServiceUnavailableException;
import io.scalecube.services.sut.GreetingResponse;
import io.scalecube.services.sut.GreetingServiceImpl;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.test.StepVerifier;

public class ErrorFlowTest extends BaseTest {

  private static final AtomicInteger PORT = new AtomicInteger(4000);

  private static Microservices provider;
  private static Microservices consumer;

  @BeforeAll
  public static void initNodes() {
    provider =
        Microservices.builder()
            .discovery(
                "provider",
                endpoint ->
                    new ScalecubeServiceDiscovery(endpoint)
                        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                        .transport(cfg -> cfg.port(PORT.incrementAndGet())))
            .transport(RSocketServiceTransport::new)
            .services(new GreetingServiceImpl())
            .startAwait();

    final Address seedAddress = provider.discovery("provider").address();

    consumer =
        Microservices.builder()
            .discovery(
                "consumer",
                endpoint ->
                    new ScalecubeServiceDiscovery(endpoint)
                        .membership(cfg -> cfg.seedMembers(seedAddress))
                        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                        .transport(cfg -> cfg.port(PORT.incrementAndGet())))
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
    assertThrows(ForbiddenException.class, () -> from(req).block());
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
