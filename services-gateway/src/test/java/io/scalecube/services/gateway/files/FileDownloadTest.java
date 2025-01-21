package io.scalecube.services.gateway.files;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.scalecube.services.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.exceptions.InternalServiceException;
import io.scalecube.services.exceptions.ServiceUnavailableException;
import io.scalecube.services.files.FileServiceImpl;
import io.scalecube.services.gateway.BaseTest;
import io.scalecube.services.gateway.client.StaticAddressRouter;
import io.scalecube.services.gateway.client.websocket.WebsocketGatewayClientTransport;
import io.scalecube.services.gateway.http.HttpGateway;
import io.scalecube.services.gateway.websocket.WebsocketGateway;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.io.IOException;
import java.lang.System.Logger.Level;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.Exceptions;
import reactor.test.StepVerifier;

public class FileDownloadTest extends BaseTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(3);

  private static Microservices gateway;
  private static Microservices microservices;
  private static Address httpAddress;
  private static Address wsAddress;

  private ServiceCall serviceCall;

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
                .services(new FileServiceImpl()) // "system level" service
                .services(new ReportServiceImpl()));

    httpAddress = gateway.gateway("HTTP").address();
    wsAddress = gateway.gateway("WS").address();
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
    serviceCall =
        new ServiceCall()
            .router(new StaticAddressRouter(wsAddress))
            .transport(new WebsocketGatewayClientTransport.Builder().address(wsAddress).build());
  }

  @AfterEach
  void afterEach() {
    if (serviceCall != null) {
      serviceCall.close();
    }
  }

  @Test
  void testExportReport() {
    fail();
  }

  @Test
  void testFileExpired() {
    fail();
  }

  @Test
  void testFileIsNotExpired() {
    fail();
  }

  @Test
  void testAddWrongFile() {
    StepVerifier.create(serviceCall.api(ReportService.class).exportReportWrongFile())
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
  void testFileNotFound() {
    final var reportResponse =
        serviceCall.api(ReportService.class).exportReport(new ExportReportRequest()).block(TIMEOUT);
    assertNotNull(reportResponse, "reportResponse");
    assertNotNull(reportResponse.reportPath(), "reportResponse.reportPath");
    assertTrue(reportResponse.reportPath().matches("^v1/scalecube.endpoints/.*/files/.*$"));

    final var reportPath = reportResponse.reportPath();
    final var s = reportPath.substring(reportPath.lastIndexOf("/"));
    final var newReportPath =
        reportPath.replace(s, "/file_must_not_be_found_" + System.currentTimeMillis());

    try {
      downloadFile("http://localhost:" + httpAddress.port() + "/" + newReportPath);
    } catch (Exception e) {
      final var cause = getRootCause(e);
      assertInstanceOf(IOException.class, cause);
      final var ex = (IOException) cause;
      final var errorType = InternalServiceException.ERROR_TYPE;
      final var message = ex.getMessage();
      assertTrue(
          message.startsWith("No Content-Disposition header in response [" + errorType),
          "message: " + message);
    }
  }

  @Test
  void testWrongFileName() {
    final var reportResponse =
        serviceCall.api(ReportService.class).exportReport(new ExportReportRequest()).block(TIMEOUT);
    assertNotNull(reportResponse, "reportResponse");
    assertNotNull(reportResponse.reportPath(), "reportResponse.reportPath");
    assertTrue(reportResponse.reportPath().matches("^v1/scalecube.endpoints/.*/files/.*$"));

    final var reportPath = reportResponse.reportPath();
    final var s = reportPath.substring(reportPath.lastIndexOf("/"));
    final var newReportPath = reportPath.replace(s, "");

    try {
      downloadFile("http://localhost:" + httpAddress.port() + "/" + newReportPath);
    } catch (Exception e) {
      final var cause = getRootCause(e);
      assertInstanceOf(IOException.class, cause);
      final var ex = (IOException) cause;
      final var errorType = ServiceUnavailableException.ERROR_TYPE;
      final var message = ex.getMessage();
      assertTrue(
          message.startsWith("No Content-Disposition header in response [" + errorType),
          "message: " + message);
    }
  }

  @Test
  void testYetAnotherWrongFileName() {
    final var reportResponse =
        serviceCall.api(ReportService.class).exportReport(new ExportReportRequest()).block(TIMEOUT);
    assertNotNull(reportResponse, "reportResponse");
    assertNotNull(reportResponse.reportPath(), "reportResponse.reportPath");
    assertTrue(reportResponse.reportPath().matches("^v1/scalecube.endpoints/.*/files/.*$"));

    final var reportPath = reportResponse.reportPath();
    final var s = reportPath.substring(reportPath.lastIndexOf("/"));
    final var newReportPath = reportPath.replace(s, "/");

    try {
      downloadFile("http://localhost:" + httpAddress.port() + "/" + newReportPath);
    } catch (Exception e) {
      final var cause = getRootCause(e);
      assertInstanceOf(IOException.class, cause);
      final var ex = (IOException) cause;
      final var errorType = ServiceUnavailableException.ERROR_TYPE;
      final var message = ex.getMessage();
      assertTrue(
          message.startsWith("No Content-Disposition header in response [" + errorType),
          "message: " + message);
    }
  }

  private static Path downloadFile(String reportUrl) {
    try {
      return HttpClient.newHttpClient()
          .send(
              HttpRequest.newBuilder().uri(URI.create(reportUrl)).build(),
              HttpResponse.BodyHandlers.ofFileDownload(Path.of("target"), CREATE, WRITE))
          .body();
    } catch (Exception e) {
      throw Exceptions.propagate(e);
    }
  }

  private static Throwable getRootCause(Throwable throwable) {
    Throwable cause = throwable.getCause();
    return (cause == null) ? throwable : getRootCause(cause);
  }
}
