package io.scalecube.services.gateway.files;

import static io.scalecube.services.api.ServiceMessage.HEADER_UPLOAD_FILENAME;
import static java.nio.file.StandardOpenOption.APPEND;

import io.scalecube.services.RequestContext;
import io.scalecube.services.annotations.AfterConstruct;
import io.scalecube.services.files.AddFileRequest;
import io.scalecube.services.files.FileService;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ReportServiceImpl implements ReportService {

  private FileService fileService;

  @AfterConstruct
  private void conclude(FileService fileService) {
    this.fileService = fileService;
  }

  @Override
  public Mono<ReportResponse> exportReport(ExportReportRequest request) {
    return Mono.defer(
        () -> {
          try {
            // Generate file under correct baseDir (java.io.tmpdir)
            final var fileSize = request.fileSize() != null ? request.fileSize() : 1024;
            final var file = generateFile(Files.createTempFile("export_report_", null), fileSize);
            return fileService
                .addFile(new AddFileRequest(file, request.duration()))
                .map(s -> new ReportResponse().reportPath(s));
          } catch (Exception ex) {
            throw new RuntimeException(ex);
          }
        });
  }

  @Override
  public Mono<ReportResponse> exportReportWrongFile() {
    // Try create file under wrong baseDir ("target")
    final var file = Path.of("target", "export_report_" + System.nanoTime()).toFile();
    return fileService
        .addFile(new AddFileRequest(file))
        .map(s -> new ReportResponse().reportPath(s));
  }

  @Override
  public Mono<String> uploadReport(Flux<byte[]> fileStream) {
    return RequestContext.deferContextual()
        .flatMap(
            context ->
                Mono.using(
                    () -> Files.createTempFile("upload-", ".tmp"),
                    tempFile ->
                        fileStream
                            .flatMap(
                                bytes ->
                                    Mono.fromCallable(
                                        () -> {
                                          Files.write(tempFile, bytes, APPEND);
                                          return bytes;
                                        }))
                            .then()
                            .thenReturn(tempFile.getFileName().toString()),
                    tempFile -> {
                      // no-op
                    }));
  }

  @Override
  public Mono<String> uploadReportError(Flux<byte[]> reportStream) {
    return RequestContext.deferContextual()
        .flatMap(
            requestContext -> {
              final var filename = requestContext.header(HEADER_UPLOAD_FILENAME);
              return reportStream
                  .then()
                  .then(Mono.error(new RuntimeException("Upload report failed: " + filename)));
            });
  }

  public static File generateFile(final Path file, final long maxSize) throws IOException {
    String lineTemplate = "export report @ ";
    byte[] lineBytes;
    long totalWritten = 0;

    try (var writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
      while (totalWritten < maxSize) {
        String line = lineTemplate + System.nanoTime() + "\n";
        lineBytes = line.getBytes(StandardCharsets.UTF_8);
        writer.write(line);
        totalWritten += lineBytes.length;
      }
    }

    return file.toFile();
  }
}
