package io.scalecube.services.gateway.files;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.scalecube.services.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.auth.CredentialsSupplier;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.exceptions.InternalServiceException;
import io.scalecube.services.exceptions.ServiceUnavailableException;
import io.scalecube.services.files.FileServiceImpl;
import io.scalecube.services.gateway.client.websocket.WebsocketGatewayClientTransport;
import io.scalecube.services.gateway.http.HttpGateway;
import io.scalecube.services.gateway.websocket.WebsocketGateway;
import io.scalecube.services.routing.StaticAddressRouter;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class FileDownloadTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(3);

  private static Microservices gateway;
  private static Microservices microservices;
  private static Address httpAddress;
  private static Address wsAddress;
  private static CredentialsSupplier credentialsSupplier;

  private ServiceCall serviceCall;

  @BeforeAll
  static void beforeAll() {
    credentialsSupplier = mock(CredentialsSupplier.class);
    when(credentialsSupplier.credentials(any(String.class), anyList())).thenReturn(Mono.never());

    gateway =
        Microservices.start(
            new Context()
                .discovery(
                    serviceEndpoint ->
                        new ScalecubeServiceDiscovery()
                            .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                            .options(opts -> opts.metadata(serviceEndpoint)))
                .transport(
                    () -> new RSocketServiceTransport().credentialsSupplier(credentialsSupplier))
                .gateway(() -> HttpGateway.builder().id("HTTP").build())
                .gateway(() -> WebsocketGateway.builder().id("WS").build()));

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
                .defaultLogger("microservices")
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
            .router(StaticAddressRouter.forService(wsAddress, "app-service").build())
            .transport(WebsocketGatewayClientTransport.builder().address(wsAddress).build());
  }

  @AfterEach
  void afterEach() {
    if (serviceCall != null) {
      serviceCall.close();
    }
  }

  @Test
  void testExportReport() throws IOException {
    final var numOfLines = 1000;
    final var reportResponse =
        serviceCall
            .api(ReportService.class)
            .exportReport(new ExportReportRequest().numOfLines(numOfLines))
            .block(TIMEOUT);
    assertNotNull(reportResponse, "reportResponse");
    assertNotNull(reportResponse.reportPath(), "reportResponse.reportPath");
    assertTrue(reportResponse.reportPath().matches("^v1/endpoints/.*/files/.*$"));

    final var file =
        downloadFile("http://localhost:" + httpAddress.port() + "/" + reportResponse.reportPath());
    final var list = Files.readAllLines(file);

    assertEquals(numOfLines, list.size(), "numOfLines");
    for (String s : list) {
      assertTrue(s.startsWith("export report @"), "line: " + s);
    }
  }

  @Test
  void testFileExpired() throws InterruptedException {
    final var numOfLines = 1000;
    final var ttl = 500;
    final var reportResponse =
        serviceCall
            .api(ReportService.class)
            .exportReport(new ExportReportRequest().numOfLines(numOfLines).ttl(ttl))
            .block(TIMEOUT);
    assertNotNull(reportResponse, "reportResponse");
    assertNotNull(reportResponse.reportPath(), "reportResponse.reportPath");
    assertTrue(reportResponse.reportPath().matches("^v1/endpoints/.*/files/.*$"));

    // Download file first time

    downloadFile("http://localhost:" + httpAddress.port() + "/" + reportResponse.reportPath());

    // Await file expiration

    Thread.sleep(ttl * 3);

    // Verify that file is expired

    try {
      downloadFile("http://localhost:" + httpAddress.port() + "/" + reportResponse.reportPath());
      fail("Expected exception");
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
    assertTrue(reportResponse.reportPath().matches("^v1/endpoints/.*/files/.*$"));

    final var reportPath = reportResponse.reportPath();
    final var s = reportPath.substring(reportPath.lastIndexOf("/"));
    final var newReportPath =
        reportPath.replace(s, "/file_must_not_be_found_" + System.currentTimeMillis());

    try {
      downloadFile("http://localhost:" + httpAddress.port() + "/" + newReportPath);
      fail("Expected exception");
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
    assertTrue(reportResponse.reportPath().matches("^v1/endpoints/.*/files/.*$"));

    final var reportPath = reportResponse.reportPath();
    final var s = reportPath.substring(reportPath.lastIndexOf("/"));
    final var newReportPath = reportPath.replace(s, "");

    try {
      downloadFile("http://localhost:" + httpAddress.port() + "/" + newReportPath);
      fail("Expected exception");
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
    assertTrue(reportResponse.reportPath().matches("^v1/endpoints/.*/files/.*$"));

    final var reportPath = reportResponse.reportPath();
    final var s = reportPath.substring(reportPath.lastIndexOf("/"));
    final var newReportPath = reportPath.replace(s, "/");

    try {
      downloadFile("http://localhost:" + httpAddress.port() + "/" + newReportPath);
      fail("Expected exception");
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
