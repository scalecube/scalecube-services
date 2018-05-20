package io.scalecube.gateway.websocket;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codec.ServiceMessageDataCodec;
import io.scalecube.services.exceptions.ExceptionProcessor;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.Duration;

public class RemoteWebSocketServerEchoRunner {

  public static final Logger LOGGER = LoggerFactory.getLogger(RemoteWebSocketServerEchoRunner.class);

  /**
   * Run test runner of Websocket server.
   * @param args - program arguments if any.
   * @throws InterruptedException - in case the program was interrupted.
   */
  public static void main(String[] args) throws InterruptedException {

    Microservices gateway = Microservices.builder()
        .build().startAwait();
   
    Microservices.builder()
        .seeds(gateway.cluster().address())
        .services(new GreetingServiceImpl()).build().startAwait();

    ServiceGateway
      .builder(gateway.call()).ws().build()
      // on instance
      .start(new InetSocketAddress(8080));
    
    Thread.currentThread().join();
  }

}
