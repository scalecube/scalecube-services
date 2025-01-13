package io.scalecube.services.gateway.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.services.methods.RequestContext;
import reactor.core.publisher.Mono;

public class RoutingServiceImpl implements RoutingService {

  @Override
  public Mono<SomeResponse> find() {
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
  public Mono<SomeResponse> update(SomeRequest request) {
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
}
