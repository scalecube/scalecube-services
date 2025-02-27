package io.scalecube.services.methods;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.services.RequestContext;
import io.scalecube.services.exceptions.ForbiddenException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class StubServiceImpl implements StubService {

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

  @Override
  public Mono<Void> invokeWithRoleOnly() {
    return RequestContext.deferContextual()
        .doOnNext(
            context -> {
              if (!context.hasRole("admin")) {
                throw new ForbiddenException("Not allowed");
              }
            })
        .then();
  }

  @Override
  public Mono<Void> invokeWithPermissionsOnly() {
    return RequestContext.deferContextual()
        .doOnNext(
            context -> {
              if (!context.hasPermission("read") || !context.hasPermission("write")) {
                throw new ForbiddenException("Not allowed");
              }
            })
        .then();
  }

  @Override
  public Mono<Void> invokeWithRoleAndPermissions() {
    return RequestContext.deferContextual()
        .doOnNext(
            context -> {
              if (!context.hasRole("admin")) {
                throw new ForbiddenException("Not allowed");
              }
              if (!context.hasPermission("read") || !context.hasPermission("write")) {
                throw new ForbiddenException("Not allowed");
              }
            })
        .then();
  }
}
