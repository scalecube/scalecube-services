package io.scalecube.gateway.service;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import io.scalecube.gateway.benchmarks.BenchmarksServiceImpl;
import io.scalecube.gateway.examples.GreetingServiceImpl;
import io.scalecube.gateway.http.HttpGateway;
import io.scalecube.gateway.rsocket.websocket.RSocketWebsocketGateway;
import io.scalecube.gateway.websocket.WebsocketGateway;
import io.scalecube.services.Microservices;
import io.scalecube.services.gateway.GatewayConfig;
import java.io.File;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GatewayServiceRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(GatewayServiceRunner.class);
  private static final String DECORATOR =
      "#######################################################################";

  private static final String REPORTER_PATH = "reports/gw/metrics";

  /**
   * Main runner.
   *
   * @param args program arguments
   * @throws Exception exception thrown
   */
  public static void main(String[] args) throws Exception {
    LOGGER.info(DECORATOR);
    LOGGER.info("Starting GatewayService");
    LOGGER.info(DECORATOR);

    MetricRegistry metrics = initMetricRegistry();

    Microservices.builder()
        .gateway(GatewayConfig.builder("ws", WebsocketGateway.class).port(7070).build())
        .gateway(GatewayConfig.builder("http", HttpGateway.class).port(8080).build())
        .gateway(GatewayConfig.builder("rsws", RSocketWebsocketGateway.class).port(9090).build())
        .metrics(metrics)
        .services(new BenchmarksServiceImpl(), new GreetingServiceImpl())
        .startAwait();

    Thread.currentThread().join();
  }

  private static MetricRegistry initMetricRegistry() {
    MetricRegistry metrics = new MetricRegistry();
    File reporterDir = new File(REPORTER_PATH);
    if (!reporterDir.exists()) {
      //noinspection ResultOfMethodCallIgnored
      reporterDir.mkdirs();
    }
    CsvReporter csvReporter =
        CsvReporter.forRegistry(metrics)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .convertRatesTo(TimeUnit.SECONDS)
            .build(reporterDir);

    csvReporter.start(10, TimeUnit.SECONDS);
    return metrics;
  }
}
