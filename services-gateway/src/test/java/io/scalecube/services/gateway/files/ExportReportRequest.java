package io.scalecube.services.gateway.files;

import java.time.Duration;
import java.util.StringJoiner;

public class ExportReportRequest {

  private Integer ttl;
  private Long fileSize;

  public Integer ttl() {
    return ttl;
  }

  public ExportReportRequest ttl(Integer ttl) {
    this.ttl = ttl;
    return this;
  }

  public Duration duration() {
    return ttl() != null ? Duration.ofMillis(ttl()) : null;
  }

  public Long fileSize() {
    return fileSize;
  }

  public ExportReportRequest fileSize(Long fileSize) {
    this.fileSize = fileSize;
    return this;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ExportReportRequest.class.getSimpleName() + "[", "]")
        .add("ttl=" + ttl)
        .add("fileSize=" + fileSize)
        .toString();
  }
}
