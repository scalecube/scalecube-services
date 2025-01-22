package io.scalecube.services.gateway.files;

import java.util.StringJoiner;

public class ReportResponse {

  private String reportPath;

  public String reportPath() {
    return reportPath;
  }

  public ReportResponse reportPath(String reportPath) {
    this.reportPath = reportPath;
    return this;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ReportResponse.class.getSimpleName() + "[", "]")
        .add("reportPath='" + reportPath + "'")
        .toString();
  }
}
