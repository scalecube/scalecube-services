package io.scalecube.services.gateway.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.services.methods.RequestContext;
import reactor.core.publisher.Mono;

public class RestServiceImpl implements RestService {

  @Override
  public Mono<SomeResponse> options() {
    return RequestContext.deferContextual()
        .map(
            context -> {
              final var foo = context.pathVar("foo");
              assertNotNull(foo);
              assertNotNull(context.headers());
              assertTrue(context.headers().size() > 0);
              assertEquals("OPTIONS", context.method());
              return new SomeResponse().name(foo);
            });
  }

  @Override
  public Mono<SomeResponse> get() {
    return RequestContext.deferContextual()
        .map(
            context -> {
              final var foo = context.pathVar("foo");
              assertNotNull(foo);
              assertNotNull(context.headers());
              assertTrue(context.headers().size() > 0);
              assertEquals("GET", context.method());
              return new SomeResponse().name(foo);
            });
  }

  @Override
  public Mono<SomeResponse> head() {
    return RequestContext.deferContextual()
        .map(
            context -> {
              final var foo = context.pathVar("foo");
              assertNotNull(foo);
              assertNotNull(context.headers());
              assertTrue(context.headers().size() > 0);
              assertEquals("HEAD", context.method());
              return new SomeResponse().name(foo);
            });
  }

  @Override
  public Mono<SomeResponse> post(SomeRequest request) {
    return RequestContext.deferContextual()
        .map(
            context -> {
              assertNotNull(context.pathVar("foo"));
              assertNotNull(context.headers());
              assertTrue(context.headers().size() > 0);
              assertEquals("POST", context.method());
              return new SomeResponse().name(request.name());
            });
  }

  @Override
  public Mono<SomeResponse> put(SomeRequest request) {
    return RequestContext.deferContextual()
        .map(
            context -> {
              assertNotNull(context.pathVar("foo"));
              assertNotNull(context.headers());
              assertTrue(context.headers().size() > 0);
              assertEquals("PUT", context.method());
              return new SomeResponse().name(request.name());
            });
  }

  @Override
  public Mono<SomeResponse> patch(SomeRequest request) {
    return RequestContext.deferContextual()
        .map(
            context -> {
              assertNotNull(context.pathVar("foo"));
              assertNotNull(context.headers());
              assertTrue(context.headers().size() > 0);
              assertEquals("PATCH", context.method());
              return new SomeResponse().name(request.name());
            });
  }

  @Override
  public Mono<SomeResponse> delete(SomeRequest request) {
    return RequestContext.deferContextual()
        .map(
            context -> {
              assertNotNull(context.pathVar("foo"));
              assertNotNull(context.headers());
              assertTrue(context.headers().size() > 0);
              assertEquals("DELETE", context.method());
              return new SomeResponse().name(request.name());
            });
  }

  @Override
  public Mono<SomeResponse> trace() {
    return RequestContext.deferContextual()
        .map(
            context -> {
              final var foo = context.pathVar("foo");
              assertNotNull(foo);
              assertNotNull(context.headers());
              assertTrue(context.headers().size() > 0);
              assertEquals("TRACE", context.method());
              return new SomeResponse().name(foo);
            });
  }
}
