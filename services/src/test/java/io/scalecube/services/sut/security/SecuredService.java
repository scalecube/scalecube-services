package io.scalecube.services.sut.security;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.auth.Secured;
import reactor.core.publisher.Mono;

@Secured
@Service("secured")
public interface SecuredService {

  @ServiceMethod
  Mono<String> helloWithRequest(String name);

  @ServiceMethod
  Mono<String> helloWithPrincipal();

  @ServiceMethod
  Mono<String> helloWithRequestAndPrincipal(String name);
}
