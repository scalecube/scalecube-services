package io.scalecube.services.sut.security;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import reactor.core.publisher.Mono;

@Service("secured")
public interface SecuredService {

  // Services secured by code in method body

  @ServiceMethod
  Mono<Void> invokeWithRoleOrPermissions();

  // Services secured by annotations in method body

  @ServiceMethod
  Mono<Void> readWithAllowedRoleAnnotation();

  @ServiceMethod
  Mono<Void> writeWithAllowedRoleAnnotation();
}
