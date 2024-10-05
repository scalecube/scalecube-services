package io.scalecube.services;

import io.scalecube.services.gateway.Gateway;
import io.scalecube.services.gateway.GatewayOptions;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import reactor.core.publisher.Mono;

class GatewayBootstrap implements AutoCloseable {

  private final List<Function<GatewayOptions, Gateway>> factories = new ArrayList<>();
  private final List<Gateway> gateways = new CopyOnWriteArrayList<>();

  private GatewayBootstrap addFactory(Function<GatewayOptions, Gateway> factory) {
    this.factories.add(factory);
    return this;
  }

  private GatewayBootstrap start(Microservices microservices, GatewayOptions options) {
    for (Function<GatewayOptions, Gateway> factory : factories) {
      LOGGER.info(
          "[{}][gateway][{}][start] Starting", microservices.id(), options.id());

      try {
        final Gateway gateway = factory.apply(options).start().toFuture().get();

        gateways.add(gateway);

        LOGGER.info(
            "[{}][gateway][{}][start] Started, address: {}",
            microservices.id(),
            gateway.id(),
            gateway.address());
      } catch (Exception ex) {
        LOGGER.error(
            "[{}][gateway][{}][start] Exception occurred: {}",
            microservices.id(),
            options.id(),
            ex.toString());
        throw new RuntimeException(ex);
      }
    }

    return this;
  }

  private List<Gateway> gateways() {
    return new ArrayList<>(gateways);
  }

  private Gateway gateway(String id) {
    return gateways.stream()
        .filter(gateway -> gateway.id().equals(id))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Cannot find gateway by id=" + id));
  }

  @Override
  public void close() {
    try {
      Mono.whenDelayError(gateways.stream().map(Gateway::stop).toArray(Mono[]::new))
          .toFuture()
          .get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
