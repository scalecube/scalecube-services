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

/**
 * Represents request-specific context that holds metadata related to the current request. It acts
 * as a wrapper around an existing {@link Context} and provides additional methods for accessing and
 * managing request-specific data such as headers, principal, method information, and path
 * variables.
 */
public class RequestContext implements Context {

  private static final Logger LOGGER = LoggerFactory.getLogger(RequestContext.class);

  private final Context source;

  /** Creates new {@code RequestContext} with an empty base context. */
  public RequestContext() {
    this(Context.empty());
  }

  /**
   * Creates new {@code RequestContext} based on an existing context.
   *
   * @param source the source context to wrap
   */
  public RequestContext(Context source) {
    this.source = Objects.requireNonNull(source, "source");
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

  /**
   * Returns request headers.
   *
   * @return headers, or {@code null} if not set
   */
  public Map<String, String> headers() {
    return source.getOrDefault("headers", null);
  }

  /**
   * Puts request headers to the context.
   *
   * @param headers headers
   * @return new {@code RequestContext} instance with updated headers
   */
  public RequestContext headers(Map<String, String> headers) {
    return put("headers", headers);
  }

  /**
   * Returns request.
   *
   * @return request, or {@code null} if not set
   */
  public Object request() {
    return source.getOrDefault("request", null);
  }

  /**
   * Puts request to the context.
   *
   * @param request request
   * @return new {@code RequestContext} instance with updated request
   */
  public RequestContext request(Object request) {
    return put("request", request);
  }

  /**
   * Returns specific request header by name.
   *
   * @param name header name
   * @return header value, or {@code null} if not found
   */
  public String header(String name) {
    final var headers = headers();
    return headers != null ? headers.get(name) : null;
  }

  /**
   * Returns request method from headers.
   *
   * @return request method, or {@code null} if not found
   */
  public String requestMethod() {
    return header(HEADER_REQUEST_METHOD);
  }

  /**
   * Returns request qualifier from headers.
   *
   * @return request qualifier, or {@code null} if not found
   */
  public String requestQualifier() {
    return header(HEADER_QUALIFIER);
  }

  /**
   * Returns principal object (authenticated entity) associated with the request.
   *
   * @return {@link Principal} associated with the request, or {@code null} if not set
   */
  public Principal principal() {
    return getOrDefault("principal", null);
  }

  /**
   * Puts principal to the context.
   *
   * @param principal principal
   * @return new {@code RequestContext} instance with the updated principal
   */
  public RequestContext principal(Principal principal) {
    return put("principal", principal);
  }

  /**
   * Checks whether request context has principal.
   *
   * @return {@code true} if principal exists and is not {@link Principal#NULL_PRINCIPAL}, {@code
   *     false} otherwise
   */
  public boolean hasPrincipal() {
    final var principal = principal();
    return principal != null && principal != NULL_PRINCIPAL;
  }

  /**
   * Retrieves {@link MethodInfo} associated with the request.
   *
   * @return {@link MethodInfo} associated with the request, or {@code null} if not set
   */
  public MethodInfo methodInfo() {
    return getOrDefault("methodInfo", null);
  }

  /**
   * Puts {@link MethodInfo} associated with the request.
   *
   * @param methodInfo methodInfo
   * @return new {@code RequestContext} instance with the updated {@link MethodInfo}
   */
  public RequestContext methodInfo(MethodInfo methodInfo) {
    return put("methodInfo", methodInfo);
  }

  /**
   * Returns path variables associated with the request.
   *
   * @return path variables, or {@code null} if not set
   */
  public Map<String, String> pathVars() {
    return get("pathVars");
  }

  /**
   * Puts path variables associated with the request.
   *
   * @return path variables, or {@code null} if not set
   */
  public RequestContext pathVars(Map<String, String> pathVars) {
    return put("pathVars", pathVars);
  }

  /**
   * Returns specific path variable by name.
   *
   * @param name name of the path variable
   * @return path variable value, or {@code null} if not found
   */
  public String pathVar(String name) {
    final var pathVars = pathVars();
    return pathVars != null ? pathVars.get(name) : null;
  }

  /**
   * Returns specific path variable by name, and converts it to the specified type.
   *
   * @param name name of the path variable
   * @param type expected type of the variable
   * @param <T> type parameter
   * @return converted path variable, or {@code null} if not found
   */
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

  /**
   * Retrieves {@link RequestContext} from the reactor context in deferred manner.
   *
   * @return {@link Mono} emitting the {@code RequestContext}, or error - if it is missing
   */
  public static Mono<RequestContext> deferContextual() {
    return Mono.deferContextual(context -> Mono.just(context.get(RequestContext.class)));
  }

  /**
   * Retrieves {@link RequestContext} from the reactor context in deferred manner, and ensures that
   * caller has necessary permissions.
   *
   * <p>This method first extracts {@code RequestContext} and then performs access control checks
   * based on the associated {@link Principal}. If request lacks valid principal or does not have
   * the required role and permissions, then {@link ForbiddenException} is thrown.
   *
   * <p>Access control enforcement follows these rules:
   *
   * <ul>
   *   <li>If no principal is present, access is denied
   *   <li>If principal role is not among the method allowed roles, access is denied
   *   <li>If principal lacks any of the required permissions, access is denied
   * </ul>
   *
   * @return {@link Mono} emitting the validated {@code RequestContext}, or error - if {@code
   *     RequestContext} is missing, or if access is denied
   */
  public static Mono<RequestContext> deferSecured() {
    return Mono.deferContextual(context -> Mono.just(context.get(RequestContext.class)))
        .doOnNext(
            context -> {
              if (!context.hasPrincipal()) {
                LOGGER.warn(
                    "Insufficient permissions for secured method ({}) -- "
                        + "request context ({}) does not have principal",
                    context,
                    context.methodInfo());
                throw new ForbiddenException("Insufficient permissions");
              }

              final var principal = context.principal();
              final var methodInfo = context.methodInfo();

              if (!methodInfo.allowedRoles().contains(principal.role())) {
                LOGGER.warn(
                    "Insufficient permissions for secured method ({}) -- "
                        + "principal role '{}' is not allowed (principal: {})",
                    context.methodInfo(),
                    principal.role(),
                    principal);
                throw new ForbiddenException("Insufficient permissions");
              }

              for (var allowedPermission : methodInfo.allowedPermissions()) {
                if (!principal.hasPermission(allowedPermission)) {
                  LOGGER.warn(
                      "Insufficient permissions for secured method ({}) -- "
                          + "allowed permission '{}' is missing (principal: {})",
                      context.methodInfo(),
                      allowedPermission,
                      principal);
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
