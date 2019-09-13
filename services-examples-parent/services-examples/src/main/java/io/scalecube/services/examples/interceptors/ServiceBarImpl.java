package io.scalecube.services.examples.interceptors;

import io.scalecube.services.annotations.Inject;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.api.ServiceMessage.Builder;
import reactor.core.publisher.Mono;

public class ServiceBarImpl implements ServiceBar {

  @Inject ServiceBaz serviceBaz;

  @Override
  public Mono<ServiceMessage> bar(ServiceMessage input) {
    int data = input.data();
    Builder builder = ServiceMessage.from(input);
    return serviceBaz.baz(builder.data(2 * data).build());
  }
}
