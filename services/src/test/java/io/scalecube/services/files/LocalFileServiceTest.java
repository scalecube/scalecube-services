package io.scalecube.services.files;

import static io.scalecube.services.api.ServiceMessage.HEADER_REQUEST_METHOD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import io.scalecube.services.Microservices;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.Reflect;
import io.scalecube.services.ServiceRegistration;
import io.scalecube.services.api.ServiceMessage;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import reactor.test.StepVerifier;

class LocalFileServiceTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(3);

  @Test
  void fileServiceIsNotAnnouncedOnlyFileStreamer() {
    assertFalse(Reflect.isService(FileService.class));

    try (var microservices = Microservices.start(new Context().services(new LocalFileService()))) {
      final Set<String> namespaces =
          microservices.serviceEndpoint().serviceRegistrations().stream()
              .map(ServiceRegistration::namespace)
              .collect(Collectors.toSet());

      assertEquals(Set.of(FileStreamer.NAMESPACE), namespaces);
    }
  }

  @Test
  void addFileBeforeStartFails() throws Exception {
    final var file = Files.createTempFile("file-service-", ".txt").toFile();

    StepVerifier.create(new LocalFileService().addFile(new AddFileRequest(file)))
        .expectError(IllegalStateException.class)
        .verify(TIMEOUT);
  }

  @Test
  void addFileReturnsPathOfThisInstance() throws Exception {
    final var fileService = new LocalFileService();
    final var file = Files.createTempFile("file-service-", ".txt").toFile();

    try (var microservices = Microservices.start(new Context().services(fileService))) {
      StepVerifier.create(fileService.addFile(new AddFileRequest(file)))
          .expectNext(
              String.join(
                  "/",
                  FileStreamer.NAMESPACE,
                  microservices.serviceEndpoint().id(),
                  "files",
                  file.getName()))
          .expectComplete()
          .verify(TIMEOUT);
    }
  }

  @Test
  void addFileRejectsFileInSubdirectory(@TempDir Path baseDir) throws Exception {
    final var fileService = new LocalFileService(baseDir.toFile(), 1024, null);
    final var file =
        Files.writeString(
            Files.createDirectories(baseDir.resolve("sub")).resolve("report.txt"), "nested");

    try (var microservices = Microservices.start(new Context().services(fileService))) {
      StepVerifier.create(fileService.addFile(new AddFileRequest(file.toFile())))
          .expectError(IllegalArgumentException.class)
          .verify(TIMEOUT);
    }
  }

  @Test
  void addedFileIsReachableByItsLink(@TempDir Path baseDir) throws Exception {
    final var fileService = new LocalFileService(baseDir.toFile(), 1024, null);
    final var file = Files.writeString(baseDir.resolve("report.txt"), "content");

    try (var microservices = Microservices.start(new Context().services(fileService))) {
      final var link = fileService.addFile(new AddFileRequest(file.toFile())).block(TIMEOUT);
      final var out = new ByteArrayOutputStream();
      microservices
          .call()
          .requestMany(
              // streamFile is a GET method, as the HTTP gateway calls it
              ServiceMessage.builder().qualifier(link).header(HEADER_REQUEST_METHOD, "GET").build(),
              byte[].class)
          .filter(ServiceMessage::hasData)
          .doOnNext(message -> out.writeBytes(message.<byte[]>data()))
          .blockLast(TIMEOUT);
      assertEquals("content", out.toString(StandardCharsets.UTF_8));
    }
  }
}
