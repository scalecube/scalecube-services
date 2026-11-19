package io.scalecube.services.gateway.http;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.scalecube.services.Microservices;
import io.scalecube.services.Microservices.Context;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.locks.LockSupport;
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
import org.junit.jupiter.api.io.TempDir;

/** Rejected multipart uploads must release their parts, so their temp files get deleted. */
class HttpGatewayUploadReleaseTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(5);
  // above the form decoder's in-memory threshold, so parts are written to disk
  private static final byte[] CONTENT = new byte[64 * 1024];

  @TempDir static Path uploadDir;

  private static Microservices gateway;

  @BeforeAll
  static void beforeAll() {
    gateway =
        Microservices.start(
            new Context()
                .gateway(
                    () ->
                        HttpGateway.builder()
                            .id("HTTP")
                            .formDecoderBuilder(builder -> builder.baseDirectory(uploadDir))
                            .build()));
  }

  @AfterAll
  static void afterAll() {
    if (gateway != null) {
      gateway.close();
    }
  }

  @Test
  void twoFilesAreRejectedAndReleased() throws Exception {
    upload(
        new MultipartBody.Builder()
            .setType(MultipartBody.FORM)
            .addFormDataPart("file", "a.txt", fileBody())
            .addFormDataPart("file", "b.txt", fileBody())
            .build());

    awaitNoTempFiles();
  }

  @Test
  void fileWithTextFieldIsRejectedAndReleased() throws Exception {
    upload(
        new MultipartBody.Builder()
            .setType(MultipartBody.FORM)
            .addFormDataPart("file", "a.txt", fileBody())
            .addFormDataPart("note", "x".repeat(CONTENT.length))
            .build());

    awaitNoTempFiles();
  }

  private static RequestBody fileBody() {
    return RequestBody.create(CONTENT, MediaType.get("application/text"));
  }

  private static void upload(MultipartBody body) throws IOException {
    final var request =
        new Request.Builder()
            .url("http://localhost:" + gateway.gateway("HTTP").address().port() + "/v1/api/upload")
            .post(body)
            .build();
    try (Response response = new OkHttpClient().newCall(request).execute()) {
      assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.code(), "response.code");
    }
  }

  private static void awaitNoTempFiles() throws IOException {
    final long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (true) {
      try (Stream<Path> files = Files.list(uploadDir)) {
        final var left = files.toList();
        if (left.isEmpty()) {
          return;
        }
        if (System.nanoTime() > deadline) {
          throw new AssertionError("Temp files not deleted: " + left);
        }
      }
      LockSupport.parkNanos(Duration.ofMillis(10).toNanos());
    }
  }
}
