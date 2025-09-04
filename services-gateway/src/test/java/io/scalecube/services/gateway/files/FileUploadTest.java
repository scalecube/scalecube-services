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
import java.nio.file.Files;
import java.util.StringJoiner;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

public class FileUploadTest {

  private static final int NUM_OF_LINES = 10000;

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
    final var client = new OkHttpClient();
    final var file = generateFile(Files.createTempFile("export_report_", null), NUM_OF_LINES);

    final var body =
        new MultipartBody.Builder()
            .setType(MultipartBody.FORM)
            .addFormDataPart(
                "file", file.getName(), RequestBody.create(file, MediaType.get("application/text")))
            .build();

    final var request =
        new Request.Builder()
            .url("http://localhost:" + httpAddress.port() + "/v1/api/uploadReport")
            .post(body)
            .build();

    try (Response response = client.newCall(request).execute()) {
      assertEquals(HttpResponseStatus.OK.code(), response.code(), "response.code");
      assertEquals(
          new StringJoiner("", "\"", "\"").add(file.getName()).toString(),
          response.body().string(),
          "response.body");
    }
  }

  @Test
  public void uploadFileFailed() throws Exception {
    final var client = new OkHttpClient();
    final var file = generateFile(Files.createTempFile("export_report_", null), NUM_OF_LINES);

    final var body =
        new MultipartBody.Builder()
            .setType(MultipartBody.FORM)
            .addFormDataPart(
                "file", file.getName(), RequestBody.create(file, MediaType.get("application/text")))
            .build();

    final var request =
        new Request.Builder()
            .url("http://localhost:" + httpAddress.port() + "/v1/api/uploadReportError")
            .post(body)
            .build();

    try (Response response = client.newCall(request).execute()) {
      assertEquals(
          HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), response.code(), "response.code");
      assertEquals(
          "{\"errorCode\":500,\"errorMessage\":\"Upload report failed: %s\"}"
              .formatted(file.getName()),
          response.body().string(),
          "response.body");
    }
  }
}
