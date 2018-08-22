package io.scalecube.gateway.examples;

import io.scalecube.config.ConfigRegistry;
import io.scalecube.gateway.config.GatewayConfigRegistry;
import io.scalecube.services.Microservices;
import io.scalecube.transport.Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runner for example services.
 */
public class ExamplesRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExamplesRunner.class);
  private static final String DECORATOR =
    "***********************************************************************";

  /**
   * Main method of runner for example services.
   *
   * @param args - program arguments.
   * @throws InterruptedException - thrown if was interrupted.
   */
  public static void main(String[] args) throws InterruptedException {
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
        .services(new GreetingServiceImpl())
      .workerThreadChooser(new ExamplesWorkerThreadChooser())
        .startAwait();

    Thread.currentThread().join();
  }
}
