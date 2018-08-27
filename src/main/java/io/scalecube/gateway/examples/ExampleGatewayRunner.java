package io.scalecube.gateway.examples;

import io.scalecube.gateway.websocket.WebsocketGateway;
import io.scalecube.services.Microservices;
import io.scalecube.services.gateway.GatewayConfig;

public class ExampleGatewayRunner {

  public static void main(String[] args) throws InterruptedException {

    Microservices service =
      Microservices.builder()
        .gateway(GatewayConfig.builder("ws", WebsocketGateway.class).port(7070).build())
        .services(new GreetingServiceImpl())
        .startAwait();

    Thread.currentThread().join();
  }
}
