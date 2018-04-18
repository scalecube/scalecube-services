package io.scalecube.services;

import io.scalecube.services.ServiceCall.Call;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.transport.server.api.ServerMessageAcceptor;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class DefaultServicesMessageAcceptor implements ServerMessageAcceptor{

  private Call services;

  public DefaultServicesMessageAcceptor(Call services) {
    this.services = services;
  }

  @Override
  public Flux<ServiceMessage> requestChannel(Publisher<ServiceMessage> request) {
    return services.channel(request);
  }

  @Override
  public Flux<ServiceMessage> requestStream(ServiceMessage request) {
    return services.listen(request);
  }

  @Override
  public Mono<ServiceMessage> requestResponse(ServiceMessage request) {
    return services.requestResponse(request);
  }

  @Override
  public Mono<Void> fireAndForget(ServiceMessage request) {
    return services.fireAndForget(request);
  }

}
