package io.scalecube.services.examples.gateway;

import io.scalecube.services.gateway.Gateway;
import io.scalecube.services.gateway.GatewayOptions;
import io.scalecube.services.transport.api.Address;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import reactor.core.publisher.Mono;

public class HttpGatewayStub implements Gateway {

  private final GatewayOptions options;
  private final InetSocketAddress address;

  public HttpGatewayStub(GatewayOptions options) {
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
          System.out.println("Starting HTTP gateway...");

          return Mono.delay(Duration.ofMillis(ThreadLocalRandom.current().nextInt(100, 500)))
              .map(tick -> this)
              .doOnSuccess(gw -> System.out.println("HTTP gateway is started on " + gw.address));
        });
  }

  @Override
  public Mono<Void> stop() {
    return Mono.defer(
        () -> {
          System.out.println("Stopping HTTP gateway...");
          return Mono.empty();
        });
  }
}
