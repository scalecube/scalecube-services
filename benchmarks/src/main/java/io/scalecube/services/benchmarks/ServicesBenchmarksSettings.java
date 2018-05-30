package io.scalecube.services.benchmarks;

import java.io.File;
import java.nio.file.Paths;
import java.time.Duration;

public class ServicesBenchmarksSettings {

  private static final int N_THREADS = Runtime.getRuntime().availableProcessors();
  private static final Duration EXECUTION_TASK_TIME = Duration.ofSeconds(60);
  private static final Duration REPORTER_PERIOD = Duration.ofSeconds(10);
  private static final String CSV_REPORTER_DIRECTORY = ".";
  private static final int RESPONSE_COUNT = 100;

  private final int nThreads;
  private final Duration executionTaskTime;
  private final Duration reporterPeriod;
  private final File csvReporterDirectory;
  private final int responseCount;

  private ServicesBenchmarksSettings(Builder builder) {
    this.nThreads = builder.nThreads;
    this.executionTaskTime = builder.executionTaskTime;
    this.reporterPeriod = builder.reporterPeriod;
    this.responseCount = builder.responseCount;
    this.csvReporterDirectory = Paths.get(builder.csvReporterDirectory).toFile();
    // noinspection ResultOfMethodCallIgnored
    this.csvReporterDirectory.mkdirs();
  }

  public int nThreads() {
    return nThreads;
  }

  public Duration executionTaskTime() {
    return executionTaskTime;
  }

  public Duration reporterPeriod() {
    return reporterPeriod;
  }

  public File csvReporterDirectory() {
    return csvReporterDirectory;
  }

  public int responseCount() {
    return responseCount;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder from(String[] args) {
    Builder builder = builder();
    if (args != null) {
      for (String pair : args) {
        String[] keyValue = pair.split("=", 2);
        String key = keyValue[0];
        String value = keyValue[1];
        switch (key) {
          case "nThreads":
            builder.nThreads(Integer.parseInt(value));
            break;
          case "executionTaskTimeInSec":
            builder.executionTaskTime(Duration.ofSeconds(Long.parseLong(value)));
            break;
          case "reporterPeriodInSec":
            builder.reporterPeriod(Duration.ofSeconds(Long.parseLong(value)));
            break;
          case "csvReporterDirectory":
            builder.csvReporterDirectory(value);
            break;
          case "responseCount":
            builder.responseCount(Integer.parseInt(value));
            break;
          default:
            throw new IllegalArgumentException("unknown command: " + pair);
        }
      }
    }
    return builder;
  }

  @Override
  public String toString() {
    return "ServicesBenchmarksSettings{" +
        "nThreads=" + nThreads +
        ", executionTaskTime=" + executionTaskTime +
        ", reporterPeriod=" + reporterPeriod +
        ", csvReporterDirectory=" + csvReporterDirectory +
        ", responseCount=" + responseCount +
        '}';
  }

  public static class Builder {
    private Integer nThreads = N_THREADS;
    private Duration executionTaskTime = EXECUTION_TASK_TIME;
    private Duration reporterPeriod = REPORTER_PERIOD;
    private String csvReporterDirectory = CSV_REPORTER_DIRECTORY;
    private Integer responseCount = RESPONSE_COUNT;

    public Builder nThreads(Integer nThreads) {
      this.nThreads = nThreads;
      return this;
    }

    public Builder executionTaskTime(Duration executionTaskTime) {
      this.executionTaskTime = executionTaskTime;
      return this;
    }

    public Builder reporterPeriod(Duration reporterPeriod) {
      this.reporterPeriod = reporterPeriod;
      return this;
    }

    public Builder csvReporterDirectory(String csvReporterDirectory) {
      this.csvReporterDirectory = csvReporterDirectory;
      return this;
    }

    public Builder responseCount(Integer responseCount) {
      this.responseCount = responseCount;
      return this;
    }

    public ServicesBenchmarksSettings build() {
      return new ServicesBenchmarksSettings(this);
    }
  }
}
