package io.scalecube.services.examples.docker;

import io.scalecube.services.Microservices;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.services.transport.rsocket.RSocketTransportResources;
import io.scalecube.transport.Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExampleServiceRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExampleServiceRunner.class);

  static final int DEFAULT_DISCOVERY_PORT = 4801;

  /**
   * Runner.
   *
   * @param args args
   */
  public static void main(String[] args) {

    int discoveryPort = Integer.getInteger("discovery.port", DEFAULT_DISCOVERY_PORT);
    Address seeds = Address.from(System.getProperty("seeds", "localhost:" + discoveryPort));
    String memberHost = System.getProperty("member.host", null);
    Integer memberPort = Integer.getInteger("member.port", null);

    LOGGER.info(
        "Starting with: seeds: {}, discovery.port: {}, member.host: {}, member.port: {}",
        seeds,
        discoveryPort,
        memberHost,
        memberPort);

    Microservices microservices =
        Microservices.builder()
            .discovery(
                (serviceEndpoint) ->
                    new ScalecubeServiceDiscovery(serviceEndpoint)
                        .options(
                            opts ->
                                opts.seedMembers(seeds)
                                    .port(discoveryPort)
                                    .memberHost(memberHost)
                                    .memberPort(memberPort)))
            .transport(
                opts ->
                    opts.resources(RSocketTransportResources::new)
                        .client(RSocketServiceTransport.INSTANCE::clientTransport)
                        .server(RSocketServiceTransport.INSTANCE::serverTransport))
            .startAwait();

    microservices.onShutdown().block();
  }
}
