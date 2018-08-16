package io.scalecube.gateway.http;

import io.scalecube.services.ServiceCall;
import io.scalecube.services.gateway.Gateway;
import io.scalecube.services.gateway.GatewayConfig;
import io.scalecube.services.metrics.Metrics;

import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;

public class HttpGateway implements Gateway {

  @Override
  public Mono<InetSocketAddress> start(GatewayConfig config, ExecutorService executorService, ServiceCall.Call call,
      Metrics metrics) {
    return null;
  }

  @Override
  public Mono<Void> stop() {
    return null;
  }
}
