package io.scalecube.services.gateway.files;

import static io.scalecube.services.gateway.files.ReportServiceImpl.generateFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.scalecube.services.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.auth.CredentialsSupplier;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.gateway.http.HttpGateway;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.util.StringJoiner;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

public class FileUploadTest {

  private static Microservices gateway;
  private static Microservices microservices;
  private static Address httpAddress;
  private static CredentialsSupplier credentialsSupplier;

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
                .gateway(() -> HttpGateway.builder().id("HTTP").build()));

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
                .services(new ReportServiceImpl()));

    httpAddress = gateway.gateway("HTTP").address();
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

  @Test
  public void uploadFileSuccessfully() throws Exception {
    final var file = generateFile(Files.createTempFile("export_report_", null), 1000);
    final var fileBytes = Files.readAllBytes(file.toPath());

    // Build the multipart body manually
    final var boundary = UUID.randomUUID().toString();
    final var requestBody = requestBody(boundary, file, fileBytes);

    final var client = HttpClient.newHttpClient();

    final var request =
        HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:" + httpAddress.port() + "/v1/api/uploadReport"))
            .header("Content-Type", "multipart/form-data; boundary=" + boundary)
            .POST(HttpRequest.BodyPublishers.ofByteArray(requestBody))
            .build();

    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    assertEquals(HttpResponseStatus.OK.code(), response.statusCode(), "response.statusCode");
    assertEquals(
        new StringJoiner("", "\"", "\"").add(file.getName()).toString(),
        response.body(),
        "response.body");
  }

  @Test
  public void uploadFileFailed() throws Exception {
    final var file = generateFile(Files.createTempFile("export_report_", null), 1000);
    final var fileBytes = Files.readAllBytes(file.toPath());

    // Build the multipart body manually
    final var boundary = UUID.randomUUID().toString();
    final var requestBody = requestBody(boundary, file, fileBytes);

    final var client = HttpClient.newHttpClient();

    final var request =
        HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:" + httpAddress.port() + "/v1/api/uploadReportError"))
            .header("Content-Type", "multipart/form-data; boundary=" + boundary)
            .POST(HttpRequest.BodyPublishers.ofByteArray(requestBody))
            .build();

    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    assertEquals(
        HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
        response.statusCode(),
        "response.statusCode");
    assertEquals(
        "{\"errorCode\":500,\"errorMessage\":\"Upload report failed: %s\"}"
            .formatted(file.getName()),
        response.body(),
        "response.body");
  }

  private static byte[] requestBody(String boundary, File file, byte[] fileBytes)
      throws IOException {
    String lineFeed = "\r\n";
    StringBuilder sb = new StringBuilder();
    sb.append("--").append(boundary).append(lineFeed);
    sb.append("Content-Disposition: form-data; name=\"file\"; filename=\"")
        .append(file.getName())
        .append("\"")
        .append(lineFeed);
    sb.append("Content-Type: ").append(Files.probeContentType(file.toPath())).append(lineFeed);
    sb.append(lineFeed);

    byte[] fileHeader = sb.toString().getBytes();
    byte[] boundaryFooter = (lineFeed + "--" + boundary + "--" + lineFeed).getBytes();

    // Concatenate header + file + footer
    byte[] requestBody = new byte[fileHeader.length + fileBytes.length + boundaryFooter.length];
    System.arraycopy(fileHeader, 0, requestBody, 0, fileHeader.length);
    System.arraycopy(fileBytes, 0, requestBody, fileHeader.length, fileBytes.length);
    System.arraycopy(
        boundaryFooter,
        0,
        requestBody,
        fileHeader.length + fileBytes.length,
        boundaryFooter.length);
    return requestBody;
  }
}
