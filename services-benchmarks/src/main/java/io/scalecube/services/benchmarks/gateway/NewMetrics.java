package io.scalecube.services.benchmarks.gateway;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import io.micrometer.core.instrument.logging.LoggingRegistryConfig;
import io.micrometer.core.instrument.util.NamedThreadFactory;
import java.time.Duration;
import reactor.core.publisher.Flux;

public class NewMetrics {

  public static void main(String[] args) {
    //

    LoggingMeterRegistry logging =
        new LoggingMeterRegistry(
            new LoggingRegistryConfig() {
              @Override
              public String get(String key) {
                return null;
              }

              @Override
              public Duration step() {
                return Duration.ofSeconds(1);
              }
            },
            Clock.SYSTEM);

    logging.start(new NamedThreadFactory("metrics"));
    logging.config().commonTags("app", "test");

    Metrics.addRegistry(logging);

    Flux.interval(Duration.ofSeconds(1))
        .doOnNext(n -> System.out.println("event occurred: " + n))
        .doOnNext(n -> Metrics.counter("CounterName", "cntTag", "cntTagValue").increment())
        .take(10)
        .blockLast();
  }
}
