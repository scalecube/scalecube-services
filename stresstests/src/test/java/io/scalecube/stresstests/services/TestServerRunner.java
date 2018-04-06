package io.scalecube.stresstests.services;

import io.scalecube.services.Microservices;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.TimeUnit;

public class TestServerRunner {

  public static void main(String[] args) throws InterruptedException {
    MetricRegistry registry = new MetricRegistry();

    ConsoleReporter reporter = ConsoleReporter.forRegistry(registry)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .build();

      Microservices build = Microservices.builder()
              .port(5858)
              .services(new GreetingServiceImpl())
              .metrics(registry)
              .build();
      reporter.start(5, TimeUnit.SECONDS);
      Thread.currentThread().join();
  }
}
