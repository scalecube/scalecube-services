package io.scalecube.examples.gateway;

import io.scalecube.services.gateway.Gateway;
import io.scalecube.services.gateway.GatewayConfig;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

import reactor.core.publisher.Mono;

public class HttpStubGateway implements Gateway {

  private static final GatewayConfig defaultConfig = GatewayConfig.builder().port(8080).build();

  @Override
  public Mono<InetSocketAddress> start() {
    return start(defaultConfig);
  }

  @Override
  public Mono<InetSocketAddress> start(GatewayConfig config) {
    return Mono.defer(() -> {
      final Integer port = config.port().orElseGet(() -> defaultConfig.port().get());

      System.out.println("Starting HTTP gateway...");

      return Mono
          .delay(Duration.ofMillis(ThreadLocalRandom.current().nextInt(100, 500)))
          .map(aLong -> new InetSocketAddress(port))
          .doOnSuccess(inetSocketAddress -> System.out.println("HTTP gateway is started on " + inetSocketAddress));
    });
  }

  @Override
  public Mono<Void> stop() {
    return Mono.defer(() -> {
      System.out.println("Stopping HTTP gateway...");
      return Mono.empty();
    });
  }
}
