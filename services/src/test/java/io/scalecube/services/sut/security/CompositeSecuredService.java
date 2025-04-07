package io.scalecube.services.sut.security;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.auth.Secured;
import reactor.core.publisher.Mono;

@Secured
@Service("compositeSecured")
public interface CompositeSecuredService {

  // Services secured by code in method body

  @Secured
  @ServiceMethod
  Mono<Void> helloComposite();
}
