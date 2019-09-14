package io.scalecube.services.examples.interceptors;

import io.scalecube.services.annotations.Inject;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.api.ServiceMessage.Builder;
import reactor.core.publisher.Mono;

public class SecuredServiceFooImpl implements SecuredServiceFoo {

  @Inject ServiceBar serviceBar;

  @Override
  public Mono<ServiceMessage> securedFoo(ServiceMessage input) {
    int data = input.data();
    Builder builder = ServiceMessage.from(input);
    return serviceBar.bar(builder.data(42 * data).build());
  }
}
