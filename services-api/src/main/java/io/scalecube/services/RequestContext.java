package io.scalecube.services;

import static io.scalecube.services.api.ServiceMessage.HEADER_QUALIFIER;
import static io.scalecube.services.api.ServiceMessage.HEADER_REQUEST_METHOD;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;
import java.util.StringJoiner;
import reactor.core.publisher.Mono;

public class RequestContext {

  public static final Object NULL_PRINCIPAL = new Object();

  private final Map<String, String> headers;
  private final Object request;
  private final Object principal;
  private final Map<String, String> pathVars;

  private RequestContext(Builder builder) {
    this.headers = builder.headers;
    this.request = builder.request;
    this.principal = builder.principal;
    this.pathVars = builder.pathVars;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder from(RequestContext context) {
    return RequestContext.builder()
        .headers(context.headers())
        .request(context.request())
        .principal(context.principal())
        .pathVars(context.pathVars());
  }

  public Map<String, String> headers() {
    return headers;
  }

  public Object request() {
    return request;
  }

  public String header(String name) {
    return headers != null ? headers.get(name) : null;
  }

  public String requestMethod() {
    return header(HEADER_REQUEST_METHOD);
  }

  public String requestQualifier() {
    return header(HEADER_QUALIFIER);
  }

  public Object principal() {
    return principal;
  }

  public boolean hasPrincipal() {
    return principal != NULL_PRINCIPAL;
  }

  public Map<String, String> pathVars() {
    return pathVars;
  }

  public String pathVar(String name) {
    return pathVars != null ? pathVars.get(name) : null;
  }

  public <T> T pathVar(String name, Class<T> clazz) {
    final var s = pathVar(name);
    if (s == null) {
      return null;
    }

    if (clazz == String.class) {
      //noinspection unchecked
      return (T) s;
    }
    if (clazz == Integer.class) {
      //noinspection unchecked
      return (T) Integer.valueOf(s);
    }
    if (clazz == Long.class) {
      //noinspection unchecked
      return (T) Long.valueOf(s);
    }
    if (clazz == BigDecimal.class) {
      //noinspection unchecked
      return (T) new BigDecimal(s);
    }
    if (clazz == BigInteger.class) {
      //noinspection unchecked
      return (T) new BigInteger(s);
    }

    throw new IllegalArgumentException("Wrong pathVar: " + name);
  }

  public static Mono<RequestContext> deferContextual() {
    return Mono.deferContextual(context -> Mono.just(context.get(RequestContext.class)));
  }

  //  public static <T> Mono<T> deferSecured(Class<T> principalType) {
  //    return Mono.deferContextual(context -> Mono.just(context.get(RequestContext.class)))
  //        .map(
  //            requestContext -> {
  //              //noinspection unchecked
  //              final var p = (T) requestContext.principal();
  //              return p;
  //            });
  //  }

  @Override
  public String toString() {
    return new StringJoiner(", ", RequestContext.class.getSimpleName() + "[", "]")
        .add("headers=" + (headers != null ? "[" + headers.size() + "]" : null))
        .add("request=" + request)
        .add("principal=" + principal)
        .add("pathVars=" + pathVars)
        .toString();
  }

  public static class Builder {

    private Map<String, String> headers;
    private Object request;
    private Object principal;
    private Map<String, String> pathVars;

    private Builder() {}

    public Builder headers(Map<String, String> headers) {
      this.headers = headers;
      return this;
    }

    public Builder request(Object request) {
      this.request = request;
      return this;
    }

    public Builder principal(Object principal) {
      this.principal = principal;
      return this;
    }

    public Builder pathVars(Map<String, String> pathVars) {
      this.pathVars = pathVars;
      return this;
    }

    public RequestContext build() {
      return new RequestContext(this);
    }
  }
}
