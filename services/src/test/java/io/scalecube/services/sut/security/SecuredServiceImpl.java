package io.scalecube.services.sut.security;

import io.scalecube.services.RequestContext;
import io.scalecube.services.auth.AllowedRole;
import io.scalecube.services.auth.Secured;
import io.scalecube.services.exceptions.ForbiddenException;
import reactor.core.publisher.Mono;

@Secured
public class SecuredServiceImpl implements SecuredService {

  // Services secured by code in method body

  @Secured
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

  @Secured
  @AllowedRole(
      name = "admin",
      permissions = {"read"})
  @Override
  public Mono<Void> readWithAllowedRoleAnnotation() {
    return Mono.empty();
  }

  @Secured
  @AllowedRole(
      name = "admin",
      permissions = {"write"})
  @Override
  public Mono<Void> writeWithAllowedRoleAnnotation() {
    return Mono.empty();
  }

  @Secured
  @AllowedRole(
      name = "gateway",
      permissions = {"gateway:read"})
  @AllowedRole(
      name = "operations",
      permissions = {"operations:read"})
  @Override
  public Mono<String> invokeByMultipleRoles() {
    return Mono.empty();
  }
}
