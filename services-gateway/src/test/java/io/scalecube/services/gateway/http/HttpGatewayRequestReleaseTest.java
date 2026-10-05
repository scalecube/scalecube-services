package io.scalecube.services.gateway.http;

import static io.scalecube.services.gateway.GatewayErrorMapperImpl.ERROR_MAPPER;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.netty.buffer.ByteBuf;
import io.scalecube.services.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.gateway.EchoService;
import io.scalecube.services.gateway.EchoServiceImpl;
import io.scalecube.services.gateway.RecyclingByteBuf;
import io.scalecube.services.gateway.client.http.HttpGatewayClientTransport;
import io.scalecube.services.routing.StaticAddressRouter;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.netty.http.server.HttpServerRequest;
import reactor.test.StepVerifier;

/**
 * Request data handed to {@link ServiceCall} by {@link HttpGatewayAcceptor} is owned by the
 * service call from then on. The gateway must not release it again, whatever the outcome of the
 * call.
 */
class HttpGatewayRequestReleaseTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(3);

  private static final TrackingHandler handler = new TrackingHandler();

  private static Microservices gateway;

  private ServiceCall serviceCall;

  @BeforeAll
  static void beforeAll() {
    gateway =
        Microservices.start(
            new Context()
                .gateway(() -> HttpGateway.builder().id("HTTP").messageHandler(handler).build())
                .services(
                    ServiceInfo.fromServiceInstance(new EchoServiceImpl())
                        .errorMapper(ERROR_MAPPER)
                        .build()));
  }

  @BeforeEach
  void beforeEach() {
    handler.buffers.clear();
    final Address address = gateway.gateway("HTTP").address();
    serviceCall =
        new ServiceCall()
            .router(StaticAddressRouter.forService(address, "app-service").build())
            .transport(HttpGatewayClientTransport.builder().address(address).build());
  }

  @AfterEach
  void afterEach() {
    if (serviceCall != null) {
      serviceCall.close();
    }
  }

  @AfterAll
  static void afterAll() {
    if (gateway != null) {
      gateway.close();
    }
  }

  @Test
  void serviceSuccessReleasesRequestDataOnce() {
    StepVerifier.create(serviceCall.api(EchoService.class).echoOne("hello"))
        .expectNext("hello")
        .expectComplete()
        .verify(TIMEOUT);

    assertReleasedOnce();
  }

  @Test
  void serviceErrorReleasesRequestDataOnce() {
    StepVerifier.create(serviceCall.api(EchoService.class).failOne("hello"))
        .expectError()
        .verify(TIMEOUT);

    assertReleasedOnce();
  }

  @Test
  void unknownServiceReleasesRequestDataOnce() {
    final ServiceMessage request =
        ServiceMessage.builder().qualifier("unknown/one").data("hello").build();

    StepVerifier.create(serviceCall.requestOne(request, String.class))
        .expectError()
        .verify(TIMEOUT);

    assertReleasedOnce();
  }

  private static void assertReleasedOnce() {
    assertEquals(1, handler.buffers.size(), "tracked request buffers");
    assertEquals(1, handler.buffers.get(0).deallocations(), "request data deallocations");
  }

  /** Replaces request data with a {@link RecyclingByteBuf} copy to count its releases. */
  private static class TrackingHandler implements HttpGatewayMessageHandler {

    private final List<RecyclingByteBuf> buffers = new CopyOnWriteArrayList<>();

    @Override
    public ServiceMessage mapMessage(HttpServerRequest request, ServiceMessage message) {
      if (!(message.data() instanceof ByteBuf data) || !data.isReadable()) {
        return message;
      }
      final RecyclingByteBuf tracked = new RecyclingByteBuf(data.readableBytes());
      tracked.writeBytes(data);
      data.release();
      buffers.add(tracked);
      return ServiceMessage.from(message).data(tracked).build();
    }
  }
}
