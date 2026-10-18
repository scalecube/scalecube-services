package io.scalecube.services.files;

import io.scalecube.services.Microservices;
import io.scalecube.services.RequestContext;
import io.scalecube.services.annotations.AfterConstruct;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.api.ServiceMessage;
import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * {@link FileService} that keeps added files on the local disk of this microservices instance and
 * serves them as {@link FileStreamer}, under {@code v1/endpoints/${microservices:id}/files/:name}.
 * Each file is deleted after its time-to-live. Register the instance on a {@code Microservices}
 * instance (it binds to its endpoint id on start) and pass the same instance to the services that
 * add files.
 *
 * <p>The class carries {@link Service} so that setups which pick the objects to register by that
 * annotation (e.g. dependency injection containers) pick it up too; the service itself is the
 * {@link FileStreamer} interface.
 */
@Service
public class LocalFileService implements FileService, FileStreamer {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalFileService.class);

  private static final int DEFAULT_CHUNK_SIZE = 64 * 1024;
  private static final String DEFAULT_DIR = System.getProperty("java.io.tmpdir");
  private static final Duration DEFAULT_TTL = Duration.ofSeconds(600);

  private final Path baseDir;
  private final int chunkSize;
  private final Duration fileTtl;

  private volatile String serviceEndpointId;

  /**
   * Creates a file service that accepts files under {@code java.io.tmpdir}, streams them in 64 KiB
   * chunks and deletes them after 10 minutes.
   */
  public LocalFileService() {
    this(new File(DEFAULT_DIR), DEFAULT_CHUNK_SIZE, DEFAULT_TTL);
  }

  /**
   * Creates a file service.
   *
   * @param baseDir directory that added files must be under
   * @param chunkSize maximum size of a chunk when a file is streamed
   * @param fileTtl default file lifetime, used when a request has no {@code ttl}; {@code null}
   *     means forever
   */
  public LocalFileService(File baseDir, int chunkSize, Duration fileTtl) {
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
          if (serviceEndpointId == null) {
            throw new IllegalStateException("Not registered on a started microservices instance");
          }
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
  public Flux<ServiceMessage> streamFile() {
    return RequestContext.deferContextual()
        .flatMapMany(
            context -> {
              final var headers = context.headers();
              final var pathParams = context.pathParams();
              final var filename = pathParams.getString("filename");
              final var path = baseDir.resolve(filename);

              if (!isPathValid(path)) {
                return Flux.error(new FileNotFoundException("File not found: " + filename));
              }

              final var message =
                  ServiceMessage.from(headers)
                      .header("Content-Type", "application/octet-stream")
                      .header("Content-Disposition", "attachment; filename=" + filename)
                      .build();

              return Flux.just(message)
                  .concatWith(
                      FileChannelFlux.createFrom(path, chunkSize)
                          .map(bytes -> ServiceMessage.from(headers).data(bytes).build()));
            });
  }

  private boolean isPathValid(Path filePath) {
    return Files.exists(filePath)
        && !Files.isDirectory(filePath)
        && filePath.normalize().startsWith(baseDir.normalize());
  }
}
