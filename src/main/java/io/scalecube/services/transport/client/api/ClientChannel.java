package io.scalecube.services.transport.client.api;

import io.scalecube.services.api.ServiceMessage;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ClientChannel {

  /**
   * Request Response pattern.
   * send single request and expect single reply.
   * 
   * @param request
   * @return
   */
  public Mono<ServiceMessage> requestReply(ServiceMessage request) ;

  /**
   * Request Stream pattern.
   * send single request and expect stream of replies.
   * 
   * @param request
   * @return
   */
  public Flux<ServiceMessage> listen(ServiceMessage request);
  
  

}
