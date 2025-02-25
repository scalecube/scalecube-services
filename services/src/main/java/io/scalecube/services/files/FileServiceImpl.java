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

  private static final int DEFAULT_MAX_CHUNK_SIZE = 64 * 1024;
  private static final String TEMP_DIR = System.getProperty("java.io.tmpdir");

  private final Path baseDir;
  private final int maxChunkSize;

  private String serviceEndpointId;

  /**
   * Default constructor. {@code baseDir} will be {@code java.io.tmpdir}, {@code maxChunkSize} will
   * be {@code 64k}.
   */
  public FileServiceImpl() {
    this(new File(TEMP_DIR), DEFAULT_MAX_CHUNK_SIZE);
  }

  /**
   * Constructor.
   *
   * @param baseDir baseDir for storing files
   * @param maxChunkSize maximum buffer size for reading file and publishing by flux
   */
  public FileServiceImpl(File baseDir, int maxChunkSize) {
    this.baseDir = baseDir.toPath();
    this.maxChunkSize = maxChunkSize;
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

          if (ttl != null && ttl != Duration.ZERO) {
            final var scheduler = Schedulers.single();
            scheduler.schedule(
                () -> {
                  if (!file.delete()) {
                    LOGGER.warn("Cannot delete file: {}", file);
                  }
                },
                ttl.toMillis(),
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
                return fluxFrom(filePath, ByteBuffer.allocate(maxChunkSize));
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
          } catch (Throwable e) {
            LOGGER.warn("Cannot close file: {}", filePath);
          }
        });
  }

  private boolean isPathValid(Path filePath) {
    return Files.exists(filePath)
        && !Files.isDirectory(filePath)
        && filePath.normalize().startsWith(baseDir.normalize());
  }
}
