package io.scalecube.gateway.examples;

import io.scalecube.config.ConfigRegistry;
import io.scalecube.gateway.benchmarks.BenchmarksServiceImpl;
import io.scalecube.gateway.config.GatewayConfigRegistry;
import io.scalecube.services.Microservices;
import io.scalecube.services.transport.api.Address;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
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
    LOGGER.info("Starting Examples services on {}", config);
    LOGGER.info(DECORATOR);

    int numOfThreads =
        Optional.ofNullable(config.numOfThreads())
            .orElse(Runtime.getRuntime().availableProcessors());
    LOGGER.info("Number of worker threads: " + numOfThreads);

    Microservices.builder()
        .discovery(
            options ->
                options
                    .seeds(
                        Arrays.stream(config.seedAddresses())
                            .map(address -> Address.create(address.host(), address.port()))
                            .toArray(Address[]::new))
                    .port(config.discoveryPort())
                    .memberHost(config.memberHost())
                    .memberPort(config.memberPort()))
        .transport(options -> options.numOfThreads(numOfThreads).port(config.servicePort()))
        .services(new BenchmarksServiceImpl(), new GreetingServiceImpl())
        .startAwait();

    Thread.currentThread().join();
  }

  public static class Config {

    private int servicePort;
    private int discoveryPort;
    private Integer numOfThreads;
    private List<String> seeds;
    private String memberHost;
    private Integer memberPort;

    public int servicePort() {
      return servicePort;
    }

    public int discoveryPort() {
      return discoveryPort;
    }

    public Integer numOfThreads() {
      return numOfThreads;
    }

    public List<String> seeds() {
      return seeds;
    }

    /**
     * Returns seeds as an {@link Address}'s array.
     *
     * @return {@link Address}'s array
     */
    public Address[] seedAddresses() {
      return Optional.ofNullable(seeds())
          .map(seeds -> seeds.stream().map(Address::from).toArray(Address[]::new))
          .orElse(new Address[0]);
    }

    public String memberHost() {
      return memberHost;
    }

    public Integer memberPort() {
      return memberPort;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("Config{");
      sb.append("servicePort=").append(servicePort);
      sb.append(", discoveryPort=").append(discoveryPort);
      sb.append(", numOfThreads=").append(numOfThreads);
      sb.append(", seeds=").append(seeds);
      sb.append(", memberHost=").append(memberHost);
      sb.append(", memberPort=").append(memberPort);
      sb.append('}');
      return sb.toString();
    }
  }
}
