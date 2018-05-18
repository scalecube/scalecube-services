package io.scalecube.gateway.websocket;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codec.ServiceMessageDataCodec;
import io.scalecube.services.exceptions.ExceptionProcessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class WebSocketServerEchoRunner {

  public static final Logger LOGGER = LoggerFactory.getLogger(WebSocketServerEchoRunner.class);

  public static void main(String[] args) throws InterruptedException {

    GreetingService serviceInstance = new GreetingServiceImpl();

    Microservices services = Microservices.builder()
        .services(serviceInstance).build().startAwait();
    ServiceCall.Call call = services.call();
    LOGGER.info("Started services at address: {}", services.serviceAddress());

    ServiceMessageDataCodec dataCodec = new ServiceMessageDataCodec();

    WebSocketAcceptor acceptor = new WebSocketAcceptor() {

      @Override
      public Mono<Void> onConnect(WebSocketSession session) {
        Flux<ServiceMessage> respStream = session
            .receive().log("###.receive()")
            .compose(call::requestBidirectional).log("###.transform()")
            .onErrorResume(throwable -> Mono.just(ExceptionProcessor.toMessage(throwable)));
        return session
            .send(respStream.map(dataCodec::encode))
            .then();
      }

      @Override
      public Mono<Void> onDisconnect(WebSocketSession session) {
        return Mono.never();
      }
    };

    WebSocketServer server = new WebSocketServer(acceptor);
    server.start(new InetSocketAddress(8080));
    Thread.currentThread().join();
  }

}
