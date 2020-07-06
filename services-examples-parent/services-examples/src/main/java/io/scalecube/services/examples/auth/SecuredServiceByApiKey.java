package io.scalecube.services.examples.auth;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.auth.Secured;
import reactor.core.publisher.Mono;

@Secured
@Service
public interface SecuredServiceByApiKey {

  @ServiceMethod
  Mono<String> hello(String name);
}
