package io.scalecube.services.files;

import io.scalecube.services.Microservices;
import io.scalecube.services.RequestContext;
import io.scalecube.services.annotations.AfterConstruct;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class FileServiceImpl implements FileService, FileStreamer {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileServiceImpl.class);

  private static final int DEFAULT_CHUNK_SIZE = 64 * 1024;
  private static final String DEFAULT_DIR = System.getProperty("java.io.tmpdir");
  private static final Duration DEFAULT_TTL = Duration.ofSeconds(600);

  private final Path baseDir;
  private final int chunkSize;
  private final Duration fileTtl;

  private String serviceEndpointId;

  public FileServiceImpl() {
    this(new File(DEFAULT_DIR), DEFAULT_CHUNK_SIZE, DEFAULT_TTL);
  }

  /**
   * Constructor.
   *
   * @param baseDir baseDir for storing files
   * @param chunkSize maximum buffer size for reading file and publishing by flux
   * @param fileTtl file lifetime, {@code null} means forever
   */
  public FileServiceImpl(File baseDir, int chunkSize, Duration fileTtl) {
    this.baseDir = baseDir.toPath();
    this.chunkSize = chunkSize;
    this.fileTtl = fileTtl;
  }

  @AfterConstruct
  void conclude(Microservices microservices) {
    serviceEndpointId = microservices.serviceEndpoint().id();
  }

  @Override
  public Mono<String> addFile(AddFileRequest request) {
    return Mono.fromCallable(
        () -> {
          if (request == null) {
            throw new IllegalArgumentException("Wrong request");
          }

          final var file = request.file();
          final var ttl = request.ttl();

          if (file == null) {
            throw new IllegalArgumentException("Wrong file");
          }

          if (!isPathValid(file.toPath())) {
            throw new IllegalArgumentException("Wrong file: " + file);
          }

          final var actualTtl = ttl != null ? ttl : fileTtl;

          if (actualTtl != null) {
            final var scheduler = Schedulers.single();
            scheduler.schedule(
                () -> {
                  if (!file.delete()) {
                    LOGGER.warn("Cannot delete file: {}", file);
                  }
                },
                actualTtl.toMillis(),
                TimeUnit.MILLISECONDS);
          }

          return String.join(
              "/", FileStreamer.NAMESPACE, serviceEndpointId, "files", file.getName());
        });
  }

  @Override
  public Flux<byte[]> streamFile() {
    return RequestContext.deferContextual()
        .flatMapMany(
            context -> {
              final var name = context.pathVar("name");
              final var filePath = baseDir.resolve(name);
              if (!isPathValid(filePath)) {
                return Flux.error(new FileNotFoundException("File not found: " + name));
              } else {
                return fluxFrom(filePath, ByteBuffer.allocate(chunkSize));
              }
            });
  }

  private static Flux<byte[]> fluxFrom(Path filePath, ByteBuffer chunkBuffer) {
    return Flux.generate(
        () -> FileChannel.open(filePath),
        (channel, sink) -> {
          try {
            int read;
            chunkBuffer.clear();
            do {
              read = channel.read(chunkBuffer);
            } while (read == 0);

            chunkBuffer.flip();
            if (chunkBuffer.remaining() > 0) {
              final var bytes = new byte[chunkBuffer.remaining()];
              chunkBuffer.get(bytes);
              sink.next(bytes);
            }

            if (read == -1) {
              sink.complete();
            }
          } catch (IOException e) {
            sink.error(e);
          }
          return channel;
        },
        channel -> {
          try {
            channel.close();
          } catch (Exception e) {
            LOGGER.warn("Cannot close file: {}", filePath, e);
          }
        });
  }

  private boolean isPathValid(Path filePath) {
    return Files.exists(filePath)
        && !Files.isDirectory(filePath)
        && filePath.normalize().startsWith(baseDir.normalize());
  }
}
