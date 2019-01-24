package io.scalecube.services.metrics;

import static io.micrometer.core.instrument.util.DoubleFormat.decimalOrWhole;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toMap;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.config.NamingConvention;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.distribution.pause.PauseDetector;
import io.micrometer.core.instrument.step.StepDistributionSummary;
import io.micrometer.core.instrument.step.StepMeterRegistry;
import io.micrometer.core.instrument.step.StepRegistryConfig;
import io.micrometer.core.instrument.step.StepTimer;
import io.micrometer.core.instrument.util.NamedThreadFactory;
import io.micrometer.core.instrument.util.TimeUtils;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link io.micrometer.core.instrument.MeterRegistry} that publishes metrics to
 * csv files (one per metric) periodically.
 */
public class CsvMeterRegistry extends StepMeterRegistry {

  private static final Logger LOGGER = LoggerFactory.getLogger(CsvMeterRegistry.class);

  private final String reportDir;
  private Duration reportInterval = Duration.ofSeconds(1);

  /**
   * Creates the an instance of the meter registry that can report metrics to CSV file.
   *
   * @param reportDir base directory for report files
   * @param reportInterval interval for publishing metrics to csv files
   */
  public CsvMeterRegistry(String reportDir, Duration reportInterval) {
    super(
        new StepRegistryConfig() {
          @Override
          public String prefix() {
            return "csv";
          }

          @Override
          public String get(String key) {
            return null;
          }

          @Override
          public Duration step() {
            return reportInterval;
          }
        },
        Clock.SYSTEM);

    this.reportDir = reportDir;
    this.reportInterval = reportInterval;
    config().namingConvention(NamingConvention.dot);
    start(new NamedThreadFactory("csv-metrics-publisher"));
  }

  @Override
  public void start(ThreadFactory threadFactory) {
    super.start(threadFactory);
  }

  @Override
  protected void publish() {
    getMeters()
        .stream()
        .sorted(
            (m1, m2) -> {
              int typeComp = m1.getId().getType().compareTo(m2.getId().getType());
              if (typeComp == 0) {
                return m1.getId().getName().compareTo(m2.getId().getName());
              }
              return typeComp;
            })
        .forEach(
            m -> {
              CsvMeterRegistry.Printer print = new CsvMeterRegistry.Printer(m);
              m.use(
                  gauge -> {
                    long timestamp = TimeUnit.MILLISECONDS.toSeconds(clock.wallTime());
                    report(timestamp, print.id(), "value", "%s", gauge.value());
                  },
                  counter -> {
                    long timestamp = TimeUnit.MILLISECONDS.toSeconds(clock.wallTime());
                    report(timestamp, print.id(), "count", "%f", counter.count());
                  },
                  timer -> {
                    long timestamp = TimeUnit.MILLISECONDS.toSeconds(clock.wallTime());
                    HistogramSnapshot snapshot = timer.takeSnapshot();
                    report(
                        timestamp,
                        print.id(),
                        "count,max,mean",
                        "%d,%f,%f",
                        snapshot.count(),
                        snapshot.max(getBaseTimeUnit()),
                        snapshot.mean(getBaseTimeUnit()));
                  },
                  summary -> {
                    long timestamp = TimeUnit.MILLISECONDS.toSeconds(clock.wallTime());
                    HistogramSnapshot snapshot = summary.takeSnapshot();
                    report(
                        timestamp,
                        print.id(),
                        "count,max,mean",
                        "%d,%s,%f",
                        snapshot.count(),
                        snapshot.max(getBaseTimeUnit()),
                        snapshot.mean(getBaseTimeUnit()));
                  },
                  longTaskTimer -> {
                    long timestamp = TimeUnit.MILLISECONDS.toSeconds(clock.wallTime());
                    report(
                        timestamp,
                        print.id(),
                        "active,duration",
                        "%d,%d",
                        longTaskTimer.activeTasks(),
                        longTaskTimer.duration(getBaseTimeUnit()));
                  },
                  timeGauge -> {
                    long timestamp = TimeUnit.MILLISECONDS.toSeconds(clock.wallTime());
                    report(
                        timestamp, print.id(), "value", "%d", timeGauge.value(getBaseTimeUnit()));
                  },
                  counter -> {
                    long timestamp = TimeUnit.MILLISECONDS.toSeconds(clock.wallTime());
                    report(timestamp, print.id(), "throughput", "%s", print.rate(counter.count()));
                  },
                  timer -> {
                    long timestamp = TimeUnit.MILLISECONDS.toSeconds(clock.wallTime());
                    report(
                        timestamp,
                        print.id(),
                        "throughput,mean",
                        "%s, %d",
                        print.rate(timer.count()),
                        print.time(timer.mean(getBaseTimeUnit())));
                  },
                  meter -> {
                    long timestamp = TimeUnit.MILLISECONDS.toSeconds(clock.wallTime());
                    Map<String, String> keyVals =
                        StreamSupport.stream(meter.measure().spliterator(), false)
                            .collect(
                                toMap(
                                    ms -> ms.getStatistic().name(),
                                    ms -> decimalOrWhole(ms.getValue())));

                    StringJoiner headersJoiner = new StringJoiner(",");
                    StringJoiner lineJoiner = new StringJoiner(",");
                    keyVals.forEach(
                        (k, v) -> {
                          headersJoiner.add(k);
                          lineJoiner.add("%s");
                        });
                    report(
                        timestamp,
                        print.id(),
                        headersJoiner.toString(),
                        lineJoiner.toString(),
                        keyVals.values().toArray());
                  });
            });
  }

  @Override
  protected Timer newTimer(
      Meter.Id id,
      DistributionStatisticConfig distributionStatisticConfig,
      PauseDetector pauseDetector) {
    return new StepTimer(
        id,
        clock,
        distributionStatisticConfig,
        pauseDetector,
        getBaseTimeUnit(),
        this.reportInterval.toMillis(),
        false);
  }

  @Override
  protected DistributionSummary newDistributionSummary(
      Meter.Id id, DistributionStatisticConfig distributionStatisticConfig, double scale) {
    return new StepDistributionSummary(
        id, clock, distributionStatisticConfig, scale, reportInterval.toMillis(), false);
  }

  @Override
  protected TimeUnit getBaseTimeUnit() {
    return TimeUnit.MILLISECONDS;
  }

  // CSV stuff
  private void report(long timestamp, String name, String header, String line, Object... values) {
    try {
      File dir = Paths.get(reportDir).toFile();
      dir.mkdirs();
      File file = new File(dir, name + ".csv");
      final boolean fileAlreadyExists = file.exists();
      if (fileAlreadyExists || file.createNewFile()) {
        final PrintWriter out =
            new PrintWriter(new OutputStreamWriter(new FileOutputStream(file, true), UTF_8));
        try {
          if (!fileAlreadyExists) {
            out.println("t," + header);
          }
          out.printf(
              Locale.getDefault(),
              String.format(Locale.getDefault(), "%d,%s%n", timestamp, line),
              values);
        } catch (Throwable th) {
          th.printStackTrace();
        } finally {
          out.close();
        }
      }
    } catch (IOException e) {
      LOGGER.debug("Error writing to {}", name, e);
    }
  }

  private class Printer {
    private final Meter meter;
    private final String baseUnit;

    private Printer(Meter meter) {
      this.meter = meter;
      String unit = meter.getId().getBaseUnit();
      this.baseUnit = unit == null ? "" : " " + unit;
    }

    String id() {
      return getConventionName(meter.getId());
    }

    String time(double time) {
      return TimeUtils.format(
          Duration.ofNanos(
              (long) TimeUtils.convert(time, getBaseTimeUnit(), TimeUnit.NANOSECONDS)));
    }

    String rate(double rate) {
      return humanReadableBaseUnit(rate / (double) reportInterval.getSeconds()) + "/s";
    }

    private String humanReadableByteCount(double bytes) {
      int unit = 1024;
      if (bytes < unit) {
        return bytes + " B";
      }
      ;
      int exp = (int) (Math.log(bytes) / Math.log(unit));
      String pre = "KMGTPE".charAt(exp - 1) + "i";
      return String.format("%.2f %sB", bytes / Math.pow(unit, exp), pre);
    }

    private String humanReadableBaseUnit(double value) {
      if (" bytes".equals(baseUnit)) {
        return humanReadableByteCount(value);
      }
      return decimalOrWhole(value) + baseUnit;
    }
  }
}
