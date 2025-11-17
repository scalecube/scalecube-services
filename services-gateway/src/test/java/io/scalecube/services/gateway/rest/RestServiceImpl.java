package io.scalecube.services.gateway.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.services.RequestContext;
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
              assertEquals("OPTIONS", context.requestMethod());
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
              assertEquals("GET", context.requestMethod());
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
              assertEquals("HEAD", context.requestMethod());
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
              assertEquals("POST", context.requestMethod());
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
              assertEquals("PUT", context.requestMethod());
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
              assertEquals("PATCH", context.requestMethod());
              return new SomeResponse().name(request.name());
            });
  }

  @Override
  public Mono<SomeResponse> delete() {
    return RequestContext.deferContextual()
        .map(
            context -> {
              final var foo = context.pathVar("foo");
              assertNotNull(foo);
              assertNotNull(context.headers());
              assertTrue(context.headers().size() > 0);
              assertEquals("DELETE", context.requestMethod());
              return new SomeResponse().name(foo);
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
              assertEquals("TRACE", context.requestMethod());
              return new SomeResponse().name(foo);
            });
  }
}
