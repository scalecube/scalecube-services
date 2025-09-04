package io.scalecube.services.files;

import java.io.IOException;
import java.nio.file.*;
import java.util.Arrays;
import java.util.Random;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

class FileChannelFluxTest {

  private Path tempFile;

  @BeforeEach
  void setup() throws IOException {
    tempFile = Files.createTempFile("filechannel", ".bin");
  }

  @AfterEach
  void cleanup() throws IOException {
    Files.deleteIfExists(tempFile);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 128, 1024, 64 * 1024, 1024 * 1024, 10 * 1024 * 1024})
  void testFileReading(int chunkSize) throws IOException {
    byte[] original = new byte[chunkSize * 3 + chunkSize / 2];
    new Random(42).nextBytes(original);
    Files.write(tempFile, original, StandardOpenOption.TRUNCATE_EXISTING);

    Flux<byte[]> flux = FileChannelFlux.createFrom(tempFile, chunkSize);

    StepVerifier.create(
            flux.collectList()
                .map(
                    chunks -> {
                      int total = chunks.stream().mapToInt(b -> b.length).sum();
                      byte[] reconstructed = new byte[total];
                      int pos = 0;
                      for (byte[] chunk : chunks) {
                        System.arraycopy(chunk, 0, reconstructed, pos, chunk.length);
                        pos += chunk.length;
                      }
                      return reconstructed;
                    }))
        .expectNextMatches(reconstructed -> Arrays.equals(original, reconstructed))
        .verifyComplete();
  }

  @Test
  void testEmptyFile() throws IOException {
    Files.write(tempFile, new byte[0], StandardOpenOption.TRUNCATE_EXISTING);

    Flux<byte[]> flux = FileChannelFlux.createFrom(tempFile, 16);

    StepVerifier.create(flux).expectComplete().verify();
  }

  @Test
  void testFileSmallerThanChunk() throws IOException {
    byte[] content = "hello-world".getBytes();
    Files.write(tempFile, content, StandardOpenOption.TRUNCATE_EXISTING);

    Flux<byte[]> flux = FileChannelFlux.createFrom(tempFile, 1024);

    StepVerifier.create(flux)
        .expectNextMatches(chunk -> Arrays.equals(chunk, content))
        .expectComplete()
        .verify();
  }
}
