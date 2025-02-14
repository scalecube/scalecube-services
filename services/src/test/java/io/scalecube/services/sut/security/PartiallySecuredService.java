package io.scalecube.services.sut.security;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.auth.Secured;
import reactor.core.publisher.Mono;

@Service("partiallySecured")
public interface PartiallySecuredService {

  @ServiceMethod
  Mono<String> publicMethod(String name);

  @Secured
  @ServiceMethod
  Mono<String> securedMethod(String name);
}
