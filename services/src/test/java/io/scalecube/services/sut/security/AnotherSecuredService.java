package io.scalecube.services.sut.security;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.auth.Secured;
import reactor.core.publisher.Mono;

@Secured
@Service(AnotherSecuredService.SERVICE_NAME)
public interface AnotherSecuredService {

  String SERVICE_NAME = "anotherSecured";

  @ServiceMethod
  Mono<String> helloWithRequest(String name);

  @ServiceMethod
  Mono<String> helloWithPrincipal();

  @ServiceMethod
  Mono<String> helloWithRequestAndPrincipal(String name);
}
