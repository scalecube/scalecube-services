package io.scalecube.services.gateway.files;

import io.scalecube.services.annotations.RequestType;
import io.scalecube.services.annotations.ResponseType;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.annotations.Tag;
import io.scalecube.services.api.ServiceMessage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service("v1/api")
public interface ReportService {

  @ServiceMethod
  Mono<ReportResponse> exportReport(ExportReportRequest request);

  @ServiceMethod
  Mono<ReportResponse> exportReportWrongFile();

  @RequestType(byte[].class)
  @ServiceMethod
  Mono<String> uploadReport(Flux<byte[]> reportStream);

  @RequestType(byte[].class)
  @ServiceMethod
  Mono<String> uploadReportError(Flux<byte[]> reportStream);

  @Tag(key = "Content-Type", value = "application/file")
  @ResponseType(byte[].class)
  @ServiceMethod
  Flux<ServiceMessage> immediateErrorOnDownload();

  @Tag(key = "Content-Type", value = "application/file")
  @ResponseType(byte[].class)
  @ServiceMethod
  Flux<ServiceMessage> emptyOnDownload();

  @Tag(key = "Content-Type", value = "application/file")
  @ResponseType(byte[].class)
  @ServiceMethod
  Flux<ServiceMessage> missingContentDispositionHeaderOnDownload();

  @Tag(key = "Content-Type", value = "application/file")
  @ResponseType(byte[].class)
  @ServiceMethod("successfulDownload/:fileSize")
  Flux<ServiceMessage> successfulDownload();
}
