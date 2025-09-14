package io.scalecube.services.gateway.files;

import static io.scalecube.services.gateway.files.ReportServiceImpl.generateFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Function;
import java.util.stream.Stream;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Mono;

public class FileUploadTest {

  private static final int MAX_SIZE = 10 * 1024 * 1024;
  private static final int SIZE_OVER_LIMIT = MAX_SIZE << 1;

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
                .gateway(
                    () ->
                        HttpGateway.builder()
                            .id("HTTP")
                            .formDecoderBuilder(builder -> builder.maxSize(MAX_SIZE))
                            .build()));

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

  @ParameterizedTest(name = "Upload successfully, size: {0} bytes")
  @ValueSource(longs = {0, 512, 1024, 1024 * 1024, 10 * 1024 * 1024})
  void uploadSuccessfully(long fileSize) throws Exception {
    final var client = new OkHttpClient();
    final var file = generateFile(Files.createTempFile("export_report_", null), fileSize);

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
      final var responseBody = response.body().string();
      assertNotNull(responseBody, "response.body");
      final var uploadFile = new File("/tmp", responseBody.replace("\"", "")).toPath();
      assertFilesEqual(file.toPath(), uploadFile);
    }
  }

  @ParameterizedTest
  @MethodSource("uploadFailedSource")
  void uploadFailed(UploadFailedArgs args) throws Exception {
    final var client = new OkHttpClient();
    final var file = generateFile(Files.createTempFile("export_report_", null), args.fileSize());

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
          "{\"errorCode\":%s,\"errorMessage\":\"%s\"}"
              .formatted(args.errorCode(), args.errorMessageFunc().apply(file)),
          response.body().string(),
          "response.body");
    }
  }

  @Test
  void onlySingleFileAllowed() throws Exception {
    final var client = new OkHttpClient();

    final var fileSize = 1024 * 1024;
    final var file1 = generateFile(Files.createTempFile("export_report_1_", null), fileSize);
    final var file2 = generateFile(Files.createTempFile("export_report_2_", null), fileSize);

    final var body =
        new MultipartBody.Builder()
            .setType(MultipartBody.FORM)
            .addFormDataPart(
                "file",
                file1.getName(),
                RequestBody.create(file1, MediaType.get("application/text")))
            .addFormDataPart(
                "file",
                file2.getName(),
                RequestBody.create(file2, MediaType.get("application/text")))
            .build();

    final var request =
        new Request.Builder()
            .url("http://localhost:" + httpAddress.port() + "/v1/api/uploadReportError")
            .post(body)
            .build();

    try (Response response = client.newCall(request).execute()) {
      assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.code(), "response.code");
      assertEquals(
          "{"
              + "\"errorCode\":400,"
              + "\"errorMessage\":\"Exactly one file-upload part is expected (received: 2)\""
              + "}",
          response.body().string(),
          "response.body");
    }
  }

  private static void assertFilesEqual(Path expected, Path actual) {
    try {
      long mismatch = Files.mismatch(expected, actual);
      if (mismatch != -1) {
        fail("Files differ at byte position " + mismatch);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static Stream<UploadFailedArgs> uploadFailedSource() {
    return Stream.of(
        new UploadFailedArgs(
            1024 * 1024,
            HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
            file -> "Upload report failed: " + file.getName()),
        new UploadFailedArgs(
            SIZE_OVER_LIMIT,
            HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
            file -> "java.io.IOException: Size exceed allowed maximum capacity"));
  }

  private record UploadFailedArgs(
      long fileSize, int errorCode, Function<File, String> errorMessageFunc) {}
}
