package io.scalecube.examples.gateway;

import io.scalecube.services.ServiceCall.Call;
import io.scalecube.services.gateway.Gateway;
import io.scalecube.services.gateway.GatewayConfig;
import io.scalecube.services.metrics.Metrics;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import reactor.core.publisher.Mono;

public class WebsocketStubGateway implements Gateway {

  public static final String WS_SPECIFIC_OPTION_NAME = "ws.specific.option";

  @Override
  public Mono<InetSocketAddress> start(
      GatewayConfig config,
      Executor workerThreadPool,
      boolean preferNative,
      Call call,
      Metrics metrics) {

    return Mono.defer(
        () -> {
          System.out.println("Starting WS gateway...");

          return Mono.delay(Duration.ofMillis(ThreadLocalRandom.current().nextInt(100, 500)))
              .map(tick -> new InetSocketAddress(config.port()))
              .doOnSuccess(address -> System.out.println("WS gateway is started on " + address));
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
