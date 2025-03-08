package io.scalecube.services.methods;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.services.RequestContext;
import io.scalecube.services.auth.AllowedRole;
import io.scalecube.services.exceptions.ForbiddenException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class StubServiceImpl implements StubService {

  // Invocation methods

  @Override
  public Mono<String> invokeOneReturnsNull() {
    return RequestContext.deferContextual().then(Mono.empty());
  }

  @Override
  public Flux<String> invokeManyReturnsNull() {
    return RequestContext.deferContextual().thenMany(Flux.empty());
  }

  @Override
  public Flux<String> invokeBidirectionalReturnsNull(Flux<String> request) {
    return RequestContext.deferContextual().thenMany(Flux.empty());
  }

  @Override
  public Mono<String> invokeOneThrowsException() {
    return RequestContext.deferContextual().then(Mono.error(new RuntimeException("Error")));
  }

  @Override
  public Flux<String> invokeManyThrowsException() {
    return RequestContext.deferContextual().thenMany(Flux.error(new RuntimeException("Error")));
  }

  @Override
  public Flux<String> invokeBidirectionalThrowsException(Flux<String> request) {
    return RequestContext.deferContextual().thenMany(Flux.error(new RuntimeException("Error")));
  }

  @Override
  public Mono<Void> invokeDynamicQualifier() {
    return RequestContext.deferContextual()
        .doOnNext(
            context -> {
              assertNotNull(context.headers(), "headers");
              assertNotNull(context.principal(), "principal");
              assertNotNull(context.pathVars(), "pathVars");
              assertNotNull(context.pathVar("foo"), "pathVar[foo]");
              assertNotNull(context.pathVar("bar"), "pathVar[bar]");
            })
        .then();
  }

  // Secured methods

  @Override
  public Mono<Void> invokeWithAuthContext() {
    return RequestContext.deferContextual()
        .doOnNext(
            context -> {
              assertNotNull(context.principal());
              assertTrue(context.hasPrincipal());
            })
        .then();
  }

  // Services secured by code in method body

  @Override
  public Mono<Void> invokeWithRoleOrPermissions() {
    return RequestContext.deferContextual()
        .doOnNext(
            context -> {
              if (!context.hasPrincipal()) {
                throw new ForbiddenException("Not allowed");
              }

              final var principal = context.principal();
              final var role = principal.role();
              final var permissions = principal.permissions();

              if (role == null && permissions == null) {
                throw new ForbiddenException("Not allowed");
              }
              if (role != null && !role.equals("invoker")) {
                throw new ForbiddenException("Not allowed");
              }
              if (permissions != null && !permissions.contains("invoke")) {
                throw new ForbiddenException("Not allowed");
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
