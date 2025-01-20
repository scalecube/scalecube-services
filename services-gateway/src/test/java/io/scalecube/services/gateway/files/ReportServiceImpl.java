package io.scalecube.services.gateway.files;

import io.scalecube.services.annotations.AfterConstruct;
import io.scalecube.services.files.AddFileRequest;
import io.scalecube.services.files.FileService;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.IntStream;
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
            final var numOfLines = request.numOfLines() != null ? request.numOfLines() : 10000;
            final var file =
                generateFile(
                    Files.createTempFile("export_report_" + System.nanoTime(), null), numOfLines);
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

  private static File generateFile(final Path file, final int numOfLines) throws IOException {
    final var list =
        IntStream.range(0, numOfLines)
            .mapToObj(i -> "export report @ " + System.nanoTime())
            .toList();
    Files.write(file, list);
    return file.toFile();
  }
}
