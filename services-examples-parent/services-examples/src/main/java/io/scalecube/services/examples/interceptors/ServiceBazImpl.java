package io.scalecube.services.examples.interceptors;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.api.ServiceMessage.Builder;
import reactor.core.publisher.Mono;

public class ServiceBazImpl implements ServiceBaz {

  @Override
  public Mono<ServiceMessage> baz(ServiceMessage input) {
    int data = input.data();
    Builder builder = ServiceMessage.from(input);
    return Mono.just(builder.data(2 * data).build());
  }
}
