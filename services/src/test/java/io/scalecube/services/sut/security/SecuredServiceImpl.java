package io.scalecube.services.sut.security;

import io.scalecube.services.RequestContext;
import io.scalecube.services.auth.AllowedRole;
import io.scalecube.services.exceptions.ForbiddenException;
import reactor.core.publisher.Mono;

public class SecuredServiceImpl implements SecuredService {

  // Services secured by code in method body

  @Override
  public Mono<Void> invokeWithRoleOrPermissions() {
    return RequestContext.deferContextual()
        .doOnNext(
            context -> {
              if (!context.hasPrincipal()) {
                throw new ForbiddenException("Insufficient permissions");
              }

              final var principal = context.principal();
              final var role = principal.role();
              final var permissions = principal.permissions();

              if (role == null && permissions == null) {
                throw new ForbiddenException("Insufficient permissions");
              }
              if (role != null && !role.equals("invoker") && !role.equals("caller")) {
                throw new ForbiddenException("Insufficient permissions");
              }
              if (permissions != null && !permissions.contains("invoke")) {
                throw new ForbiddenException("Insufficient permissions");
              }
            })
        .then();
  }

  // Services secured by annotations in method body

  @AllowedRole(
      name = "admin",
      permissions = {"read", "write"})
  @Override
  public Mono<Void> invokeWithAllowedRoleAnnotation() {
    return RequestContext.deferSecured().then();
  }
}
