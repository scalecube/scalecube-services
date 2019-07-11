package io.scalecube.services.sut.security;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.auth.Auth;
import reactor.core.publisher.Mono;

@Service(PartiallySecuredService.SERVICE_NAME)
public interface PartiallySecuredService {

  String SERVICE_NAME = "partiallySecured";

  @ServiceMethod
  Mono<String> publicMethod(String name);

  @ServiceMethod
  @Auth
  Mono<String> securedMethod(String name);
}
