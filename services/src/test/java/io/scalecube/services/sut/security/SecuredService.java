package io.scalecube.services.sut.security;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.auth.Auth;
import io.scalecube.services.auth.Principal;
import reactor.core.publisher.Mono;

@Service(SecuredService.SERVICE_NAME)
@Auth
public interface SecuredService {

  String SERVICE_NAME = "secured";

  @ServiceMethod
  Mono<String> helloWithRequest(String name);

  @ServiceMethod
  Mono<String> helloWithPrincipal(@Principal UserProfile user);

  @ServiceMethod
  Mono<String> helloWithRequestAndPrincipal(String name, @Principal UserProfile user);
}
