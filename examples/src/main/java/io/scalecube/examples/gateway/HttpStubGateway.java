package io.scalecube.examples.gateway;

import io.scalecube.services.ServiceCall;
import io.scalecube.services.gateway.Gateway;
import io.scalecube.services.gateway.GatewayConfig;
import io.scalecube.services.metrics.Metrics;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import reactor.core.publisher.Mono;

public class HttpStubGateway implements Gateway {

  @Override
  public Mono<InetSocketAddress> start(
      GatewayConfig config,
      ExecutorService executorService,
      ServiceCall.Call call,
      Metrics metrics) {

    return Mono.defer(
        () -> {
          System.out.println("Starting HTTP gateway...");

          return Mono.delay(Duration.ofMillis(ThreadLocalRandom.current().nextInt(100, 500)))
              .map(aLong -> new InetSocketAddress(config.port()))
              .doOnSuccess(address -> System.out.println("HTTP gateway is started on " + address));
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
