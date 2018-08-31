package io.scalecube.gateway.examples;

import io.scalecube.config.ConfigRegistry;
import io.scalecube.gateway.benchmarks.BenchmarksServiceImpl;
import io.scalecube.gateway.config.GatewayConfigRegistry;
import io.scalecube.services.Microservices;
import io.scalecube.transport.Address;
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

    ExamplesConfig config =
        configRegistry.objectValue("io.scalecube.gateway.examples", ExamplesConfig.class, null);

    LOGGER.info(DECORATOR);
    LOGGER.info("Starting Examples services on " + config);
    LOGGER.info(DECORATOR);

    int servicePort = config.getServicePort();
    Address[] seeds = config.getSeedAddress().stream().map(Address::from).toArray(Address[]::new);

    Microservices.builder()
        .seeds(seeds)
        .servicePort(servicePort)
        .services(new BenchmarksServiceImpl())
        .startAwait();

    Thread.currentThread().join();
  }
}
