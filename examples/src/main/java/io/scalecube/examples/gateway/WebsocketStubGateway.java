package io.scalecube.examples.gateway;

import io.scalecube.services.gateway.Gateway;
import io.scalecube.services.gateway.GatewayConfig;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

import reactor.core.publisher.Mono;

public class WebsocketStubGateway implements Gateway {

  public static final String WS_SPECIFIC_OPTION_NAME = "ws.specific.option";

  private static final GatewayConfig defaultConfig = GatewayConfig.builder()
      .port(9090)
      .addOption(WS_SPECIFIC_OPTION_NAME, "100")
      .build();

  @Override
  public Mono<InetSocketAddress> start() {
    return start(defaultConfig);
  }

  @Override
  public Mono<InetSocketAddress> start(GatewayConfig config) {
    return Mono.defer(() -> {
      final Integer port = config.port().orElseGet(() -> defaultConfig.port().get());

      System.out.println("Starting WS gateway...");

      return Mono
          .delay(Duration.ofMillis(ThreadLocalRandom.current().nextInt(100, 500)))
          .map(aLong -> new InetSocketAddress(port))
          .doOnSuccess(inetSocketAddress -> System.out.println("WS gateway is started on " + inetSocketAddress));
    });
  }

  @Override
  public Mono<Void> stop() {
    return Mono.defer(() -> {
      System.out.println("Stopping WS gateway...");
      return Mono.empty();
    });
  }
}
