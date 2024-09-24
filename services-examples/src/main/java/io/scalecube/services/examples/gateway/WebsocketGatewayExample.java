package io.scalecube.services.examples.gateway;

import io.scalecube.services.Address;
import io.scalecube.services.gateway.Gateway;
import io.scalecube.services.gateway.GatewayOptions;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import reactor.core.publisher.Mono;

public class WebsocketGatewayExample implements Gateway {

  private final GatewayOptions options;
  private final InetSocketAddress address;

  public WebsocketGatewayExample(GatewayOptions options) {
    this.options = options;
    this.address = new InetSocketAddress(options.port());
  }

  @Override
  public String id() {
    return options.id();
  }

  @Override
  public Address address() {
    return Address.create(address.getHostString(), address.getPort());
  }

  @Override
  public Mono<Gateway> start() {
    return Mono.defer(
        () -> {
          System.out.println("Starting WS gateway...");

          return Mono.delay(Duration.ofMillis(ThreadLocalRandom.current().nextInt(100, 500)))
              .map(tick -> this)
              .doOnSuccess(gw -> System.out.println("WS gateway is started on " + gw.address));
        });
  }

  @Override
  public Mono<Void> stop() {
    return Mono.defer(
        () -> {
          System.out.println("Stopping WS gateway...");
          return Mono.empty();
        });
  }
}
