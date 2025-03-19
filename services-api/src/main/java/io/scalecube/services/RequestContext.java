package io.scalecube.services;

import static io.scalecube.services.api.ServiceMessage.HEADER_QUALIFIER;
import static io.scalecube.services.api.ServiceMessage.HEADER_REQUEST_METHOD;
import static io.scalecube.services.auth.Principal.NULL_PRINCIPAL;

import io.scalecube.services.auth.Principal;
import io.scalecube.services.exceptions.ForbiddenException;
import io.scalecube.services.methods.MethodInfo;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

public class RequestContext implements Context {

  private static final Logger LOGGER = LoggerFactory.getLogger(RequestContext.class);

  private final Context source;

  public RequestContext() {
    this(Context.empty());
  }

  public RequestContext(Context source) {
    this.source = source;
  }

  @Override
  public RequestContext put(Object key, Object value) {
    Objects.requireNonNull(key, "key");
    //noinspection ConstantValue
    return value != null ? new RequestContext(source.put(key, value)) : this;
  }

  @Override
  public <T> T get(Object key) {
    if (key == RequestContext.class) {
      //noinspection unchecked
      return (T) this;
    } else {
      return source.get(key);
    }
  }

  @Override
  public <T> T get(Class<T> key) {
    if (key == RequestContext.class) {
      //noinspection unchecked
      return (T) this;
    } else {
      return source.get(key);
    }
  }

  @Override
  public boolean hasKey(Object key) {
    return key == RequestContext.class || source.hasKey(key);
  }

  @Override
  public int size() {
    return source.size();
  }

  @Override
  public Stream<Entry<Object, Object>> stream() {
    return source.put(RequestContext.class, this).stream();
  }

  @Override
  public Context delete(Object key) {
    return key == RequestContext.class ? source : source.delete(key);
  }

  public Map<String, String> headers() {
    return source.getOrDefault("headers", null);
  }

  public RequestContext headers(Map<String, String> headers) {
    return put("headers", headers);
  }

  public Object request() {
    return source.getOrDefault("request", null);
  }

  public RequestContext request(Object request) {
    return put("request", request);
  }

  public String header(String name) {
    final var headers = headers();
    return headers != null ? headers.get(name) : null;
  }

  public String requestMethod() {
    return header(HEADER_REQUEST_METHOD);
  }

  public String requestQualifier() {
    return header(HEADER_QUALIFIER);
  }

  public Principal principal() {
    return getOrDefault("principal", null);
  }

  public RequestContext principal(Principal principal) {
    return put("principal", principal);
  }

  public boolean hasPrincipal() {
    final var principal = principal();
    return principal != null && principal != NULL_PRINCIPAL;
  }

  public MethodInfo methodInfo() {
    return getOrDefault("methodInfo", null);
  }

  public RequestContext methodInfo(MethodInfo methodInfo) {
    return put("methodInfo", methodInfo);
  }

  public Map<String, String> pathVars() {
    return get("pathVars");
  }

  public RequestContext pathVars(Map<String, String> pathVars) {
    return put("pathVars", pathVars);
  }

  public String pathVar(String name) {
    final var pathVars = pathVars();
    return pathVars != null ? pathVars.get(name) : null;
  }

  public <T> T pathVar(String name, Class<T> type) {
    final var s = pathVar(name);
    if (s == null) {
      return null;
    }

    if (type == String.class) {
      //noinspection unchecked
      return (T) s;
    }
    if (type == Integer.class) {
      //noinspection unchecked
      return (T) Integer.valueOf(s);
    }
    if (type == Long.class) {
      //noinspection unchecked
      return (T) Long.valueOf(s);
    }
    if (type == BigDecimal.class) {
      //noinspection unchecked
      return (T) new BigDecimal(s);
    }
    if (type == BigInteger.class) {
      //noinspection unchecked
      return (T) new BigInteger(s);
    }

    throw new IllegalArgumentException("Wrong pathVar type: " + type);
  }

  public static Mono<RequestContext> deferContextual() {
    return Mono.deferContextual(context -> Mono.just(context.get(RequestContext.class)));
  }

  public static Mono<RequestContext> deferSecured() {
    return Mono.deferContextual(context -> Mono.just(context.get(RequestContext.class)))
        .doOnNext(
            context -> {
              if (!context.hasPrincipal()) {
                if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug(
                      "Insufficient permissions for secured method ({}) -- "
                          + "request context ({}) does not have principal",
                      context,
                      context.methodInfo());
                }
                throw new ForbiddenException("Insufficient permissions");
              }

              final var principal = context.principal();
              final var methodInfo = context.methodInfo();

              if (!methodInfo.allowedRoles().contains(principal.role())) {
                if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug(
                      "Insufficient permissions for secured method ({}) -- "
                          + "principal role '{}' is not allowed (principal: {})",
                      context.methodInfo(),
                      principal.role(),
                      principal);
                }
                throw new ForbiddenException("Insufficient permissions");
              }

              for (var allowedPermission : methodInfo.allowedPermissions()) {
                if (!principal.hasPermission(allowedPermission)) {
                  if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                        "Insufficient permissions for secured method ({}) -- "
                            + "allowed permission '{}' is missing (principal: {})",
                        context.methodInfo(),
                        allowedPermission,
                        principal);
                  }
                  throw new ForbiddenException("Insufficient permissions");
                }
              }
            });
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", RequestContext.class.getSimpleName() + "[", "]")
        .add("source=" + source)
        .toString();
  }
}
