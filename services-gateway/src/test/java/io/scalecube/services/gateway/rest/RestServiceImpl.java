package io.scalecube.services.gateway.rest;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.scalecube.services.RequestContext;
import java.util.UUID;
import reactor.core.publisher.Mono;

public class RestServiceImpl implements RestService {

  @Override
  public Mono<SomeResponse> options() {
    return RequestContext.deferContextual()
        .map(
            context -> {
              final var pathParams = context.pathParams();
              final var foo = pathParams.stringValue("foo");
              assertNotNull(foo);
              assertNotNull(context.headers());
              assertThat(context.headers().size(), greaterThan(0));
              assertEquals("OPTIONS", context.requestMethod());
              return new SomeResponse().name(foo);
            });
  }

  @Override
  public Mono<SomeResponse> get() {
    return RequestContext.deferContextual()
        .map(
            context -> {
              final var pathParams = context.pathParams();
              final var foo = pathParams.stringValue("foo");
              assertNotNull(foo);
              assertNotNull(context.headers());
              assertThat(context.headers().size(), greaterThan(0));
              assertEquals("GET", context.requestMethod());
              return new SomeResponse().name(foo);
            });
  }

  @Override
  public Mono<SomeResponse> head() {
    return RequestContext.deferContextual()
        .map(
            context -> {
              final var pathParams = context.pathParams();
              final var foo = pathParams.stringValue("foo");
              assertEquals("head123456", foo, "pathParam");
              final var headers = context.headers();
              assertNotNull(headers);
              assertThat(context.headers().size(), greaterThan(0));
              assertEquals("HEAD", context.requestMethod());

              assertThat(
                  headers,
                  allOf(
                      hasKey("http.method"),
                      hasKey("http.header.X-Custom-Header-1"),
                      hasKey("http.header.X-Custom-Header-2"),
                      hasKey("http.query.param1"),
                      hasKey("http.query.param2")));

              return new SomeResponse().name(foo);
            });
  }

  @Override
  public Mono<SomeResponse> post(SomeRequest request) {
    return RequestContext.deferContextual()
        .map(
            context -> {
              final var pathParams = context.pathParams();
              final var foo = pathParams.stringValue("foo");
              assertNotNull(foo);
              assertNotNull(context.headers());
              assertThat(context.headers().size(), greaterThan(0));
              assertEquals("POST", context.requestMethod());
              return new SomeResponse().name(request.name());
            });
  }

  @Override
  public Mono<SomeResponse> put(SomeRequest request) {
    return RequestContext.deferContextual()
        .map(
            context -> {
              final var pathParams = context.pathParams();
              final var foo = pathParams.stringValue("foo");
              assertNotNull(foo);
              assertNotNull(context.headers());
              assertThat(context.headers().size(), greaterThan(0));
              assertEquals("PUT", context.requestMethod());
              return new SomeResponse().name(request.name());
            });
  }

  @Override
  public Mono<SomeResponse> patch(SomeRequest request) {
    return RequestContext.deferContextual()
        .map(
            context -> {
              final var pathParams = context.pathParams();
              final var foo = pathParams.stringValue("foo");
              assertNotNull(foo);
              assertNotNull(context.headers());
              assertThat(context.headers().size(), greaterThan(0));
              assertEquals("PATCH", context.requestMethod());
              return new SomeResponse().name(request.name());
            });
  }

  @Override
  public Mono<SomeResponse> delete() {
    return RequestContext.deferContextual()
        .map(
            context -> {
              final var pathParams = context.pathParams();
              final var foo = pathParams.stringValue("foo");
              assertNotNull(foo);
              assertNotNull(context.headers());
              assertThat(context.headers().size(), greaterThan(0));
              assertEquals("DELETE", context.requestMethod());
              return new SomeResponse().name(foo);
            });
  }

  @Override
  public Mono<SomeResponse> trace() {
    return RequestContext.deferContextual()
        .map(
            context -> {
              final var pathParams = context.pathParams();
              final var foo = pathParams.stringValue("foo");
              assertNotNull(foo);
              assertNotNull(context.headers());
              assertThat(context.headers().size(), greaterThan(0));
              assertEquals("TRACE", context.requestMethod());
              return new SomeResponse().name(foo);
            });
  }

  @Override
  public Mono<SomeResponse> propagateRequestAttributes() {
    return RequestContext.deferContextual()
        .map(
            context -> {
              final var pathParams = context.pathParams();
              assertEquals(123, pathParams.intValue("foo"), "foo");
              assertEquals("bar456", pathParams.stringValue("bar"), "bar");
              assertEquals("baz789", pathParams.stringValue("baz"), "baz");

              final var headers = context.headers();
              assertEquals("GET", headers.get("http.method"));
              assertEquals("abc", headers.get("http.header.X-String-Header"));
              assertEquals("123456789", headers.get("http.header.X-Int-Header"));
              assertEquals("true", headers.get("http.query.debug"));
              assertEquals("1", headers.get("http.query.x"));
              assertEquals("2", headers.get("http.query.y"));

              final var httpHeaders = context.headerParams("http.header");
              assertEquals("abc", httpHeaders.stringValue("X-String-Header"));
              assertEquals(123456789, httpHeaders.intValue("X-Int-Header"));

              final var queryParams = context.headerParams("http.query");
              assertEquals(true, queryParams.booleanValue("debug"));
              assertEquals(1, queryParams.intValue("x"));
              assertEquals(2, queryParams.intValue("y"));

              return new SomeResponse().name(UUID.randomUUID().toString());
            });
  }
}
