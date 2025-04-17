package io.scalecube.services.sut.security;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import reactor.core.publisher.Mono;

@Service("compositeSecured")
public interface CompositeSecuredService {

  // Services secured by code in method body

  @ServiceMethod
  Mono<Void> helloComposite();
}
