package io.scalecube.services.gateway.files;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyString;
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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
    when(credentialsSupplier.credentials(anyString(), anyString())).thenReturn(Mono.never());

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

  @Nested
  class FileServiceTests {

    @ParameterizedTest(name = "Download file of size {0} bytes")
    @ValueSource(longs = {0, 512, 1024, 1024 * 1024, 10 * 1024 * 1024})
    void testDownloadSuccessfully(long fileSize) throws Exception {
      final var reportResponse =
          serviceCall
              .api(ReportService.class)
              .exportReport(new ExportReportRequest().fileSize(fileSize))
              .block(TIMEOUT);
      assertNotNull(reportResponse, "reportResponse");
      assertNotNull(reportResponse.reportPath(), "reportResponse.reportPath");
      assertTrue(reportResponse.reportPath().matches("^v1/endpoints/.*/files/.*$"));

      final var file =
          downloadFile(
              "http://localhost:" + httpAddress.port() + "/" + reportResponse.reportPath());
      assertTrue(Files.exists(file), "File does not exist");
      assertTrue(file.toFile().length() >= fileSize, "fileSize: " + file.toFile().length());
      for (String s : Files.readAllLines(file)) {
        assertTrue(s.startsWith("export report @"), "line: " + s);
      }
    }

    @Test
    void testFileExpired() throws Exception {
      final var ttl = 500;
      final var reportResponse =
          serviceCall
              .api(ReportService.class)
              .exportReport(new ExportReportRequest().ttl(ttl))
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
    void testWrongFile() {
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
          serviceCall
              .api(ReportService.class)
              .exportReport(new ExportReportRequest())
              .block(TIMEOUT);
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
    void testWrongFilename() {
      final var reportResponse =
          serviceCall
              .api(ReportService.class)
              .exportReport(new ExportReportRequest())
              .block(TIMEOUT);
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
    void testAnotherWrongFilename() {
      final var reportResponse =
          serviceCall
              .api(ReportService.class)
              .exportReport(new ExportReportRequest())
              .block(TIMEOUT);
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
  }

  @Nested
  class DirectDownloadTests {

    @ParameterizedTest(name = "Fail on download: {0}")
    @ValueSource(
        strings = {
          "immediateErrorOnDownload",
          "emptyOnDownload",
          "missingContentDispositionHeaderOnDownload"
        })
    void failOnDownload(String path) {
      final var exception =
          assertThrows(
              IOException.class,
              () -> downloadFile("http://localhost:" + httpAddress.port() + "/v1/api/" + path));
      assertTrue(exception.getMessage().contains("No Content-Disposition header"));
    }

    @ParameterizedTest(name = "Download file of size {0} bytes")
    @ValueSource(longs = {0, 512, 1024, 1024 * 1024, 10 * 1024 * 1024})
    void successfulDownload(long fileSize) throws Exception {
      final var file =
          downloadFile(
              "http://localhost:" + httpAddress.port() + "/v1/api/successfulDownload/" + fileSize);
      assertTrue(Files.exists(file), "File does not exist");
      assertTrue(file.toFile().length() >= fileSize, "fileSize: " + file.toFile().length());
      for (String s : Files.readAllLines(file)) {
        assertTrue(s.startsWith("export report @"), "line: " + s);
      }
    }
  }

  private static Path downloadFile(String reportUrl) throws Exception {
    return HttpClient.newHttpClient()
        .send(
            HttpRequest.newBuilder().uri(URI.create(reportUrl)).build(),
            HttpResponse.BodyHandlers.ofFileDownload(Path.of("target"), CREATE, WRITE))
        .body();
  }

  private static Throwable getRootCause(Throwable throwable) {
    Throwable cause = throwable.getCause();
    return (cause == null) ? throwable : getRootCause(cause);
  }
}
