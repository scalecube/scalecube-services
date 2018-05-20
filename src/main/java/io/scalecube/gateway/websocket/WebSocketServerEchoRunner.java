package io.scalecube.gateway.websocket;

import io.scalecube.services.Microservices;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class WebSocketServerEchoRunner {

  public static final Logger LOGGER = LoggerFactory.getLogger(WebSocketServerEchoRunner.class);

  /**
   * Run test runner of Websocket server.
   * @param args - program arguments if any.
   * @throws InterruptedException - in case the program was interrupted.
   */
  public static void main(String[] args) throws InterruptedException {

    GreetingService serviceInstance = new GreetingServiceImpl();

    Microservices services = Microservices.builder()
        .services(serviceInstance).build()
        .startAwait();

    ServiceGateway
      .builder(services.call()).ws().build()
      // on instance
      .start(new InetSocketAddress(8080));
    
    Thread.currentThread().join();
  }

}
