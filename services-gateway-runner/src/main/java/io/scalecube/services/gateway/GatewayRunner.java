package io.scalecube.services.gateway;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import io.scalecube.config.ConfigRegistry;
import io.scalecube.config.ConfigRegistrySettings;
import io.scalecube.config.audit.Slf4JConfigEventListener;
import io.scalecube.config.source.ClassPathConfigSource;
import io.scalecube.config.source.SystemEnvironmentConfigSource;
import io.scalecube.config.source.SystemPropertiesConfigSource;
import io.scalecube.services.Microservices;
import io.scalecube.services.Microservices.ServiceTransportBootstrap;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.discovery.ClusterAddresses;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.gateway.http.HttpGateway;
import io.scalecube.services.gateway.rsocket.RSocketGateway;
import io.scalecube.services.gateway.ws.WebsocketGateway;
import io.scalecube.services.transport.api.Address;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.services.transport.rsocket.RSocketTransportResources;
import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.regex.Pattern;
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
    ConfigRegistry configRegistry = ConfigBootstrap.configRegistry();

    Config config =
        configRegistry
            .objectProperty("io.scalecube.services.gateway", Config.class)
            .value()
            .orElseThrow(() -> new IllegalStateException("Couldn't load config"));

    LOGGER.info(DECORATOR);
    LOGGER.info("Starting Gateway on {}", config);
    LOGGER.info(DECORATOR);

    MetricRegistry metrics = initMetricRegistry();

    Microservices.builder()
        .discovery(serviceEndpoint -> serviceDiscovery(serviceEndpoint, config))
        .transport(opts -> serviceTransport(opts, config))
        .gateway(opts -> new WebsocketGateway(opts.id("ws").port(7070)))
        .gateway(opts -> new HttpGateway(opts.id("http").port(8080)).corsEnabled(true))
        .gateway(opts -> new RSocketGateway(opts.id("rsws").port(9090)))
        .metrics(metrics)
        .startAwait()
        .onShutdown()
        .block();
  }

  private static ServiceTransportBootstrap serviceTransport(
      ServiceTransportBootstrap opts, Config config) {
    return opts.resources(RSocketTransportResources::new)
        .client(RSocketServiceTransport.INSTANCE::clientTransport)
        .server(RSocketServiceTransport.INSTANCE::serverTransport)
        .port(config.servicePort());
  }

  private static ServiceDiscovery serviceDiscovery(ServiceEndpoint serviceEndpoint, Config config) {
    return new ScalecubeServiceDiscovery(serviceEndpoint)
        .options(
            opts ->
                opts.seedMembers(ClusterAddresses.toAddresses(config.seedAddresses()))
                    .port(config.discoveryPort())
                    .memberHost(config.memberHost())
                    .memberPort(config.memberPort()));
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
    private String memberHost;
    private Integer memberPort;

    public int servicePort() {
      return servicePort;
    }

    public int discoveryPort() {
      return discoveryPort;
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
      sb.append(", seeds=").append(seeds);
      sb.append(", memberHost=").append(memberHost);
      sb.append(", memberPort=").append(memberPort);
      sb.append('}');
      return sb.toString();
    }
  }

  public static class ConfigBootstrap {

    private static final Pattern CONFIG_PATTERN = Pattern.compile("(.*)config(.*)?\\.properties");
    private static final Predicate<Path> PATH_PREDICATE =
        path -> CONFIG_PATTERN.matcher(path.toString()).matches();

    /**
     * ConfigRegistry method factory.
     *
     * @return configRegistry
     */
    public static ConfigRegistry configRegistry() {
      return ConfigRegistry.create(
          ConfigRegistrySettings.builder()
              .addListener(new Slf4JConfigEventListener())
              .addLastSource("sys_prop", new SystemPropertiesConfigSource())
              .addLastSource("env_var", new SystemEnvironmentConfigSource())
              .addLastSource("cp", new ClassPathConfigSource(PATH_PREDICATE))
              .jmxEnabled(false)
              .build());
    }
  }
}
