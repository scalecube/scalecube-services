package io.scalecube.gateway.examples;

import io.scalecube.config.ConfigRegistry;
import io.scalecube.gateway.benchmarks.BenchmarksServiceImpl;
import io.scalecube.gateway.config.GatewayConfigRegistry;
import io.scalecube.services.Microservices;
import io.scalecube.transport.Address;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExamplesRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExamplesRunner.class);
  private static final String DECORATOR =
      "***********************************************************************";

  /**
   * Main method of gateway runner.
   *
   * @param args program arguments
   * @throws Exception exception thrown
   */
  public static void main(String[] args) throws Exception {
    ConfigRegistry configRegistry = GatewayConfigRegistry.configRegistry();

    Config config =
        configRegistry
            .objectProperty("io.scalecube.gateway.examples", Config.class)
            .value()
            .orElseThrow(() -> new IllegalStateException("Couldn't load config"));

    LOGGER.info(DECORATOR);
    LOGGER.info("Starting Examples services on " + config);
    LOGGER.info(DECORATOR);

    int servicePort = config.getServicePort();
    int discoveryPort = config.getDiscoveryPort();
    int numOfThreads = config.getNumOfThreads();
    Address[] seeds = config.getSeeds().stream().map(Address::from).toArray(Address[]::new);

    Microservices.builder()
        .seeds(seeds)
        .numOfThreads(numOfThreads)
        .servicePort(servicePort)
        .discoveryPort(discoveryPort)
        .services(new BenchmarksServiceImpl(), new GreetingServiceImpl())
        .startAwait();

    Thread.currentThread().join();
  }

  public static class Config {

    private int servicePort;
    private int discoveryPort;
    private int numOfThreads;
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

    public int getNumOfThreads() {
      return numOfThreads;
    }

    public void setNumOfThreads(int numOfThreads) {
      this.numOfThreads = numOfThreads;
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
      sb.append(", discoveryPort=").append(discoveryPort);
      sb.append(", numOfThreads=").append(numOfThreads);
      sb.append(", seeds=").append(seeds);
      sb.append('}');
      return sb.toString();
    }
  }
}
