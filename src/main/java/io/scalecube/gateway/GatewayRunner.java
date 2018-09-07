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
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GatewayRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(GatewayRunner.class);
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
    ConfigRegistry configRegistry = GatewayConfigRegistry.configRegistry();
    MetricRegistry metrics = initMetricRegistry();

    Config config =
        configRegistry
            .objectProperty("io.scalecube.gateway", Config.class)
            .value()
            .orElseThrow(() -> new IllegalStateException("Couldn't load config"));

    LOGGER.info(DECORATOR);
    LOGGER.info("Starting Gateway on " + config);
    LOGGER.info(DECORATOR);

    int servicePort = config.getServicePort();
    int discoveryPort = config.getDiscoveryPort();
    Address[] seeds = config.getSeeds().stream().map(Address::from).toArray(Address[]::new);

    Microservices.builder()
        .seeds(seeds)
        .servicePort(servicePort)
        .discoveryPort(discoveryPort)
        .gateway(GatewayConfig.builder("ws", WebsocketGateway.class).port(7070).build())
        .gateway(GatewayConfig.builder("http", HttpGateway.class).port(8080).build())
        .gateway(GatewayConfig.builder("rsws", RSocketWebsocketGateway.class).port(9090).build())
        .metrics(metrics)
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

  public static class Config {

    private int servicePort;
    private int discoveryPort;
    private List<String> seeds;

    public Config() {}

    public int getServicePort() {
      return servicePort;
    }

    public void setServicePort(int servicePort) {
      this.servicePort = servicePort;
    }

    public int getDiscoveryPort() {
      return discoveryPort;
    }

    public void setDiscoveryPort(int discoveryPort) {
      this.discoveryPort = discoveryPort;
    }

    public List<String> getSeeds() {
      return seeds;
    }

    public void setSeeds(List<String> seeds) {
      this.seeds = seeds;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("Config{");
      sb.append("servicePort=").append(servicePort);
      sb.append(", seeds='").append(seeds).append('\'');
      sb.append('}');
      return sb.toString();
    }
  }
}
