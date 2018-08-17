package io.scalecube.gateway.http;

import io.scalecube.config.ConfigRegistry;
import io.scalecube.gateway.config.GatewayConfigRegistry;
import io.scalecube.services.Microservices;
import io.scalecube.services.gateway.GatewayConfig;
import io.scalecube.transport.Address;

import java.util.Collections;
import java.util.List;

public class GatewayHttpRunner {

  private static final String SEEDS = "SEEDS";
  private static final List<String> DEFAULT_SEEDS = Collections.singletonList("localhost:4802");

  public static void main(String[] args) throws InterruptedException {
    final ConfigRegistry configRegistry = GatewayConfigRegistry.configRegistry();

    final Address[] seeds = configRegistry.stringListValue(SEEDS, DEFAULT_SEEDS)
        .stream().map(Address::from).toArray(Address[]::new);

    Microservices seed = Microservices.builder()
        .gateway(GatewayConfig.builder("http", HttpGateway.class).build())
        .seeds(seeds)
        .startAwait();


    Runtime.getRuntime().addShutdownHook(new Thread(() -> seed.shutdown().block()));

    Thread.currentThread().join();
  }

}
