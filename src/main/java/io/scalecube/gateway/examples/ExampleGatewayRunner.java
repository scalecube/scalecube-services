package io.scalecube.gateway.examples;

import io.scalecube.gateway.websocket.WebsocketServer;
import io.scalecube.services.Microservices;

public class ExampleGatewayRunner {

  public static void main(String[] args) throws InterruptedException {
    
    Microservices service = Microservices.builder()
        .services(new GreetingServiceImpl())
        .startAwait();
    
    WebsocketServer server = new WebsocketServer(service);
    server.start(8080);

    Thread.currentThread().join();
  }
}
