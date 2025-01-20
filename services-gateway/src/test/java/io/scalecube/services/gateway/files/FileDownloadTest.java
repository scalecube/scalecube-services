package io.scalecube.services.gateway.files;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.services.Microservices;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.exceptions.InternalServiceException;
import io.scalecube.services.gateway.BaseTest;
import io.scalecube.services.gateway.client.StaticAddressRouter;
import io.scalecube.services.gateway.client.http.HttpGatewayClientTransport;
import io.scalecube.services.gateway.client.websocket.WebsocketGatewayClientTransport;
import io.scalecube.services.gateway.http.HttpGateway;
import io.scalecube.services.gateway.websocket.WebsocketGateway;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.lang.System.Logger.Level;
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

public class FileDownloadTest extends BaseTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(3);

  private static Microservices gateway;
  private static Microservices microservices;

  private ServiceCall httpCall;
  private ServiceCall wsCall;

  @BeforeAll
  static void beforeAll() {
    gateway =
        Microservices.start(
            new Context()
                .discovery(
                    serviceEndpoint ->
                        new ScalecubeServiceDiscovery()
                            .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                            .options(opts -> opts.metadata(serviceEndpoint)))
                .transport(RSocketServiceTransport::new)
                .gateway(() -> new HttpGateway.Builder().id("HTTP").build())
                .gateway(() -> new WebsocketGateway.Builder().id("WS").build()));

    microservices =
        Microservices.start(
            new Context()
                .discovery(
                    serviceEndpoint ->
                        new ScalecubeServiceDiscovery()
                            .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                            .options(opts -> opts.metadata(serviceEndpoint))
                            .membership(
                                opts -> opts.seedMembers(gateway.discoveryAddress().toString())))
                .transport(RSocketServiceTransport::new)
                .defaultLogger("microservices", Level.INFO)
                .services(new ReportServiceImpl()));
  }

  @AfterAll
  static void afterAll() {
    if (gateway != null) {
      gateway.close();
    }
    if (microservices != null) {
      microservices.close();
    }
  }

  @BeforeEach
  void beforeEach() {
    final var httpAddress = gateway.gateway("HTTP").address();
    final var wsAddress = gateway.gateway("WS").address();

    httpCall =
        new ServiceCall()
            .router(new StaticAddressRouter(httpAddress))
            .transport(new HttpGatewayClientTransport.Builder().address(httpAddress).build());

    wsCall =
        new ServiceCall()
            .router(new StaticAddressRouter(wsAddress))
            .transport(new WebsocketGatewayClientTransport.Builder().address(wsAddress).build());
  }

  @AfterEach
  void afterEach() {
    if (httpCall != null) {
      httpCall.close();
    }
    if (wsCall != null) {
      wsCall.close();
    }
  }

  @Test
  void testExportReport() {}

  @Test
  void testAddWrongFile() {
    StepVerifier.create(wsCall.api(ReportService.class).exportReportWrongFile())
        .expectSubscription()
        .verifyErrorSatisfies(
            ex -> {
              assertInstanceOf(InternalServiceException.class, ex, "exceptionType");
              final var serviceException = (InternalServiceException) ex;
              assertEquals(500, serviceException.errorCode());
              assertTrue(serviceException.getMessage().startsWith("Wrong file: target"));
            });
  }

  @Test
  void testFileNotFound() {}

  @Test
  void testFileExpired() {}
}
