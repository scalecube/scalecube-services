package io.scalecube.services.gateway.files;

import java.time.Duration;
import java.util.StringJoiner;

public class ExportReportRequest {

  private Integer ttl;
  private Integer numOfLines;

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

  public Integer numOfLines() {
    return numOfLines;
  }

  public ExportReportRequest numOfLines(Integer numOfLines) {
    this.numOfLines = numOfLines;
    return this;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ExportReportRequest.class.getSimpleName() + "[", "]")
        .add("ttl=" + ttl)
        .add("numOfLines=" + numOfLines)
        .toString();
  }
}
