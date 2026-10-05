package io.scalecube.services.gateway.websocket;

import static io.scalecube.services.gateway.GatewayErrorMapperImpl.ERROR_MAPPER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.ByteBuf;
import io.scalecube.services.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.gateway.EchoService;
import io.scalecube.services.gateway.EchoServiceImpl;
import io.scalecube.services.gateway.GatewaySession;
import io.scalecube.services.gateway.GatewaySessionHandler;
import io.scalecube.services.gateway.RecyclingByteBuf;
import io.scalecube.services.gateway.client.websocket.WebsocketGatewayClientTransport;
import io.scalecube.services.routing.StaticAddressRouter;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

/**
 * Request data handed to {@link ServiceCall} by {@link WebsocketGatewayAcceptor} is owned by the
 * service call from then on: the rsocket transport releases it once the request frame is written
 * (remote service), the method invoker releases it on decode (local service), the service call
 * releases it if no service is found. The gateway must not release it again, whatever the outcome
 * of the stream.
 */
class WebsocketGatewayRequestReleaseTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(3);

  private static final TrackingHandler remoteHandler = new TrackingHandler();
  private static final TrackingHandler localHandler = new TrackingHandler();

  private static Microservices remoteGateway;
  private static Microservices microservices;
  private static Microservices localGateway;

  private ServiceCall serviceCall;

  @BeforeAll
  static void beforeAll() {
    remoteGateway =
        Microservices.start(
            new Context()
                .discovery(
                    serviceEndpoint ->
                        new ScalecubeServiceDiscovery()
                            .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                            .options(opts -> opts.metadata(serviceEndpoint)))
                .transport(RSocketServiceTransport::new)
                .gateway(
                    () ->
                        WebsocketGateway.builder().id("WS").gatewayHandler(remoteHandler).build()));

    microservices =
        Microservices.start(
            new Context()
                .discovery(
                    serviceEndpoint ->
                        new ScalecubeServiceDiscovery()
                            .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                            .options(opts -> opts.metadata(serviceEndpoint))
                            .membership(
                                opts ->
                                    opts.seedMembers(remoteGateway.discoveryAddress().toString())))
                .transport(RSocketServiceTransport::new)
                .services(
                    ServiceInfo.fromServiceInstance(new EchoServiceImpl())
                        .errorMapper(ERROR_MAPPER)
                        .build()));

    localGateway =
        Microservices.start(
            new Context()
                .gateway(
                    () -> WebsocketGateway.builder().id("WS").gatewayHandler(localHandler).build())
                .services(
                    ServiceInfo.fromServiceInstance(new EchoServiceImpl())
                        .errorMapper(ERROR_MAPPER)
                        .build()));
  }

  @BeforeEach
  void beforeEach() {
    remoteHandler.buffers.clear();
    localHandler.buffers.clear();
  }

  @AfterEach
  void afterEach() {
    if (serviceCall != null) {
      serviceCall.close();
    }
  }

  @AfterAll
  static void afterAll() {
    if (remoteGateway != null) {
      remoteGateway.close();
    }
    if (microservices != null) {
      microservices.close();
    }
    if (localGateway != null) {
      localGateway.close();
    }
  }

  @Test
  void remoteServiceSuccessReleasesRequestDataOnce() {
    StepVerifier.create(echoService(remoteGateway).echo("hello"))
        .expectNext("hello")
        .expectComplete()
        .verify(TIMEOUT);

    assertReleasedOnce(remoteHandler);
  }

  @Test
  void remoteServiceErrorReleasesRequestDataOnce() {
    StepVerifier.create(echoService(remoteGateway).fail("hello")).expectError().verify(TIMEOUT);

    assertReleasedOnce(remoteHandler);
  }

  @Test
  void remoteConnectionLossReleasesRequestDataOnce() throws Exception {
    final HangingServiceImpl hangingService = new HangingServiceImpl();
    final Microservices hangingNode =
        Microservices.start(
            new Context()
                .discovery(
                    serviceEndpoint ->
                        new ScalecubeServiceDiscovery()
                            .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                            .options(opts -> opts.metadata(serviceEndpoint))
                            .membership(
                                opts ->
                                    opts.seedMembers(remoteGateway.discoveryAddress().toString())))
                .transport(RSocketServiceTransport::new)
                .services(hangingService));
    try {
      await(
          () ->
              remoteGateway.serviceRegistry().listServiceReferences().stream()
                  .anyMatch(r -> r.namespace().equals("HangingService")));

      final StepVerifier verifier =
          StepVerifier.create(hangingService(remoteGateway).hang("hello")).expectError().verifyLater();
      assertTrue(hangingService.subscribed.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));

      hangingNode.close(); // drops the gateway's rsocket connection mid-stream
      verifier.verify(TIMEOUT);
    } finally {
      hangingNode.close();
    }

    assertReleasedOnce(remoteHandler);
  }

  @Test
  void unknownServiceReleasesRequestDataOnce() {
    final ServiceMessage request =
        ServiceMessage.builder().qualifier("unknown/many").data("hello").build();

    StepVerifier.create(gatewayCall(remoteGateway).requestMany(request, String.class))
        .expectError()
        .verify(TIMEOUT);

    assertReleasedOnce(remoteHandler);
  }

  @Test
  void localServiceSuccessReleasesRequestDataOnce() {
    StepVerifier.create(echoService(localGateway).echo("hello"))
        .expectNext("hello")
        .expectComplete()
        .verify(TIMEOUT);

    assertReleasedOnce(localHandler);
  }

  @Test
  void localServiceErrorReleasesRequestDataOnce() {
    StepVerifier.create(echoService(localGateway).fail("hello")).expectError().verify(TIMEOUT);

    assertReleasedOnce(localHandler);
  }

  private EchoService echoService(Microservices gateway) {
    return gatewayCall(gateway).api(EchoService.class);
  }

  private HangingService hangingService(Microservices gateway) {
    return gatewayCall(gateway).api(HangingService.class);
  }

  private ServiceCall gatewayCall(Microservices gateway) {
    final Address address = gateway.gateway("WS").address();
    serviceCall =
        new ServiceCall()
            .router(StaticAddressRouter.forService(address, "app-service").build())
            .transport(WebsocketGatewayClientTransport.builder().address(address).build());
    return serviceCall;
  }

  private static void assertReleasedOnce(TrackingHandler handler) {
    assertEquals(1, handler.buffers.size(), "tracked request buffers");
    final RecyclingByteBuf buffer = handler.buffers.get(0);
    // the transport releases on write completion, which may trail the response slightly
    await(() -> buffer.deallocations() > 0);
    assertEquals(1, buffer.deallocations(), "request data deallocations");
  }

  private static void await(Supplier<Boolean> condition) {
    final long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (!condition.get() && System.nanoTime() < deadline) {
      Thread.onSpinWait();
    }
  }

  /** Replaces request data with a {@link RecyclingByteBuf} copy to count its releases. */
  private static class TrackingHandler implements GatewaySessionHandler {

    private final List<RecyclingByteBuf> buffers = new CopyOnWriteArrayList<>();

    @Override
    public ServiceMessage mapMessage(
        GatewaySession session, ServiceMessage message, reactor.util.context.Context context) {
      if (!(message.data() instanceof ByteBuf data)) {
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
