package io.scalecube.gateway.websocket;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codec.ServiceMessageDataCodec;
import io.scalecube.services.exceptions.ExceptionProcessor;
import io.scalecube.transport.Address;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

public class WebSocketServerEchoRunner {

  public static final Logger LOGGER = LoggerFactory.getLogger(WebSocketServerEchoRunner.class);

  @Service("greeting")
  public interface ServiceHello {
    @ServiceMethod
    Mono<String> helloString(String hello);
  }

  public static class ServiceHelloImpl implements ServiceHello {
    @Override
    public Mono<String> helloString(String hello) {
      return Mono.just(hello).log("^^^^^^^^^ ServiceHelloImpl");
    }
  }

  public static void main(String[] args) throws InterruptedException {

    Microservices gateway = Microservices.builder().build().startAwait();
    Address address = gateway.cluster().address();

    LOGGER.info("address: " + address);

    Microservices services = Microservices.builder()
        .seeds(address)
        .services(new ServiceHelloImpl())
        .build()
        .startAwait();

    LOGGER.info("services: " + services);

    ServiceMessageDataCodec dataCodec = new ServiceMessageDataCodec();

    WebSocketAcceptor acceptor = new WebSocketAcceptor() {
      @Override
      public Mono<Void> onConnect(WebSocketSession session) {
        ServiceCall.Call call = services.call();

        MonoProcessor<ServiceMessage> requestProcessor = MonoProcessor.create();
        MonoProcessor<ServiceMessage> responseProcessor = MonoProcessor.create();

        session.receive().subscribe(requestProcessor::onNext);

        call.requestBidirectional(requestProcessor)
            .onErrorResume(throwable -> Mono.just(ExceptionProcessor.toMessage(throwable)))
            .map(dataCodec::encode)
            .subscribe(responseProcessor::onNext);

        return session.send(responseProcessor);
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
