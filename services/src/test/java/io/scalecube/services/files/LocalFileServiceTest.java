package io.scalecube.services.files;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import io.scalecube.services.Microservices;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.Reflect;
import io.scalecube.services.ServiceRegistration;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
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
}
