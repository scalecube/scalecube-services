package io.scalecube.services.methods;

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

  public static Mono<RequestContext> deferContextual() {
    return Mono.deferContextual(context -> Mono.just(context.get(RequestContext.class)));
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", RequestContext.class.getSimpleName() + "[", "]")
        .add("headers=" + headers)
        .add("principal=" + principal)
        .add("pathVars=" + pathVars)
        .toString();
  }
}
