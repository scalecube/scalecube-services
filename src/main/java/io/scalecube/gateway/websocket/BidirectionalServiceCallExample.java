package io.scalecube.gateway.websocket;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codec.ServiceMessageDataCodec;

import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;

/**
 * This is an example runner that proves that request-stream communication mode works correctly.
 */
public class BidirectionalServiceCallExample {

  /**
   * The main method of the class.
   * 
   * @param args - command line params if any.
   */
  public static void main(String[] args) {
    GreetingService serviceInstance = new GreetingServiceImpl();
    Microservices services = Microservices.builder().services(serviceInstance).build().startAwait();
    ServiceCall.Call call = services.call();
    ServiceMessageDataCodec dataCodec = new ServiceMessageDataCodec();

    Flux<ServiceMessage> req = Flux.interval(Duration.ofSeconds(1)).map(Long::toHexString)
        .map(n -> dataCodec.encode(ServiceMessage.builder().qualifier("greeting/helloStream").data(n).build()));

    List<ServiceMessage> list = call.create().requestBidirectional(req).log().collectList().block();
    System.out.println(list.size());
  }
}
