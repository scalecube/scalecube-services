package io.scalecube.services.benchmarks.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class ServicesBenchmarksRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServicesBenchmarksRunner.class);

  private static final int N_THREADS = Runtime.getRuntime().availableProcessors();
  private static final Duration EXECUTION_TASK_TIME = Duration.ofSeconds(20);
  private static final int RESPONSE_COUNT = 10;
  private static final Duration REPORTER_PERIOD = Duration.ofSeconds(10);
  private static final BiConsumer<ServicesBenchmarks, Settings> ONE_WAY =
      (servicesBenchmarks, settings) -> servicesBenchmarks.oneWay().take(settings.executionTaskTime).blockLast();
  private static final BiConsumer<ServicesBenchmarks, Settings> REQUEST_ONE =
      (servicesBenchmarks, settings) -> servicesBenchmarks.requestOne().take(settings.executionTaskTime).blockLast();
  private static final BiConsumer<ServicesBenchmarks, Settings> REQUEST_MANY = (servicesBenchmarks,
      settings) -> servicesBenchmarks.requestMany(settings.responseCount).take(settings.executionTaskTime).blockLast();

  public static void main(String[] args) {
    Settings settings = Settings.from(args).build();
    LOGGER.info(settings.toString());
    ServicesBenchmarks servicesBenchmarks = new ServicesBenchmarks(settings.nThreads, settings.reporterPeriod).start();

    settings.tasks.forEach(task -> task.accept(servicesBenchmarks, settings));

    servicesBenchmarks.tearDown();
  }

  public static class Settings {

    private final int nThreads;
    private final Duration executionTaskTime;
    private final int responseCount;
    private final Duration reporterPeriod;
    private final List<BiConsumer<ServicesBenchmarks, Settings>> tasks;

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
            case "responseCount":
              builder.responseCount(Integer.parseInt(value));
              break;
            case "reporterPeriodInSec":
              builder.reporterPeriod(Duration.ofSeconds(Long.parseLong(value)));
              break;
            case "methods":
              String[] methods = value.split(",");
              for (String method : methods) {
                switch (method) {
                  case "oneWay":
                    builder.addTask(ONE_WAY);
                    break;
                  case "requestOne":
                    builder.addTask(REQUEST_ONE);
                    break;
                  case "requestMany":
                    builder.addTask(REQUEST_MANY);
                    break;
                  default:
                    throw new IllegalArgumentException("unknown method: " + method);
                }
              }
              break;
            default:
              throw new IllegalArgumentException("unknown command: " + pair);
          }
        }
      }
      return builder;
    }

    private Settings(Builder builder) {
      this.nThreads = Optional.ofNullable(builder.nThreads).orElse(N_THREADS);
      this.executionTaskTime = Optional.ofNullable(builder.executionTaskTime).orElse(EXECUTION_TASK_TIME);
      this.responseCount = Optional.ofNullable(builder.responseCount).orElse(RESPONSE_COUNT);
      this.reporterPeriod = Optional.ofNullable(builder.reporterPeriod).orElse(REPORTER_PERIOD);
      this.tasks = Optional.ofNullable(builder.tasks).orElse(Arrays.asList(ONE_WAY, REQUEST_ONE, REQUEST_MANY));
    }

    @Override
    public String toString() {
      List<String> taskNames = tasks.stream().map(task -> {
        if (task == ONE_WAY) {
          return "oneWay";
        }
        if (task == REQUEST_ONE) {
          return "requestOne";
        }
        if (task == REQUEST_MANY) {
          return "requestMany";
        }
        return task.toString();
      }).collect(Collectors.toList());
      return "Settings{" +
          "nThreads=" + nThreads +
          ", executionTaskTime=" + executionTaskTime +
          ", responseCount=" + responseCount +
          ", reporterPeriod=" + reporterPeriod +
          ", tasks=" + taskNames +
          '}';
    }

    public static class Builder {
      private Integer nThreads;
      private Duration executionTaskTime;
      private Integer responseCount;
      private Duration reporterPeriod;
      private List<BiConsumer<ServicesBenchmarks, Settings>> tasks;

      public Builder nThreads(Integer nThreads) {
        this.nThreads = nThreads;
        return this;
      }

      public Builder executionTaskTime(Duration executionTaskTime) {
        this.executionTaskTime = executionTaskTime;
        return this;
      }

      public Builder responseCount(Integer responseCount) {
        this.responseCount = responseCount;
        return this;
      }

      public Builder reporterPeriod(Duration reporterPeriod) {
        this.reporterPeriod = reporterPeriod;
        return this;
      }

      public Builder addTask(BiConsumer<ServicesBenchmarks, Settings> task) {
        if (tasks == null) {
          tasks = new ArrayList<>();
        }
        tasks.add(task);
        return this;
      }

      public Settings build() {
        return new Settings(this);
      }
    }
  }
}


