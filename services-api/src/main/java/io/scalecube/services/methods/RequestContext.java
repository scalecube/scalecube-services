package io.scalecube.services.methods;

import static io.scalecube.services.api.ServiceMessage.HEADER_REQUEST_METHOD;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import reactor.core.publisher.Mono;

public class RequestContext {

  private final Map<String, String> headers;
  private final Object principal;
  private final Map<String, String> pathVars;

  /**
   * Constructor.
   *
   * @param headers message headers
   * @param principal authenticated principal (optional)
   * @param pathVars path variables (optional)
   */
  public RequestContext(
      Map<String, String> headers, Object principal, Map<String, String> pathVars) {
    this.headers = Collections.unmodifiableMap(new HashMap<>(headers));
    this.principal = principal;
    this.pathVars = pathVars != null ? Map.copyOf(pathVars) : null;
  }

  public Map<String, String> headers() {
    return headers;
  }

  public String header(String name) {
    return headers.get(name);
  }

  public String requestMethod() {
    return headers.get(HEADER_REQUEST_METHOD);
  }

  public <T> T principal() {
    //noinspection unchecked
    return (T) principal;
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

    throw new IllegalArgumentException("Wrong pathVar class: " + clazz.getName());
  }

  public static Mono<RequestContext> deferContextual() {
    return Mono.deferContextual(context -> Mono.just(context.get(RequestContext.class)));
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", RequestContext.class.getSimpleName() + "[", "]")
        .add("headers(" + headers.size() + ")")
        .add("principal=" + principal)
        .add("pathVars=" + pathVars)
        .toString();
  }
}
