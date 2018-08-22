package io.scalecube.gateway;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import io.scalecube.config.ConfigRegistry;
import io.scalecube.gateway.config.GatewayConfigRegistry;
import io.scalecube.gateway.http.HttpGateway;
import io.scalecube.gateway.rsocket.websocket.RSocketWebsocketGateway;
import io.scalecube.gateway.websocket.WebsocketGateway;
import io.scalecube.services.Microservices;
import io.scalecube.services.gateway.GatewayConfig;
import io.scalecube.transport.Address;
import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class GatewayRunner {

  private static final String REPORTER_PATH = "reports/gw/metrics";

  private static final String SEEDS = "SEEDS";
  private static final List<String> DEFAULT_SEEDS = Collections.singletonList("localhost:4802");

  /**
   * Main method of gateway runner.
   *
   * @param args program arguments
   * @throws InterruptedException interrupted exception
   */
  public static void main(String[] args) throws InterruptedException {
    ConfigRegistry configRegistry = GatewayConfigRegistry.configRegistry();
    MetricRegistry metrics = initMetricRegistry();

    final Address[] seeds =
        configRegistry
            .stringListValue(SEEDS, DEFAULT_SEEDS)
            .stream()
            .map(Address::from)
            .toArray(Address[]::new);

    Microservices.builder()
        .seeds(seeds)
        .gateway(GatewayConfig.builder("ws", WebsocketGateway.class).port(8080).build())
        .gateway(GatewayConfig.builder("http", HttpGateway.class).port(7070).build())
        .gateway(GatewayConfig.builder("rsws", RSocketWebsocketGateway.class).port(9090).build())
        .metrics(metrics)
      .workerThreadChooser(
        GatewayWorkerThreadChooser.builder()
          .addCountingChooser(8080)
          .addCountingChooser(7070)
          .addCountingChooser(9090)
          .build())
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
