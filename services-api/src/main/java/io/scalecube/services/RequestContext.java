package io.scalecube.services;

import static io.scalecube.services.MaskUtil.mask;
import static io.scalecube.services.api.ServiceMessage.HEADER_QUALIFIER;
import static io.scalecube.services.api.ServiceMessage.HEADER_REQUEST_METHOD;
import static io.scalecube.services.api.ServiceMessage.HEADER_UPLOAD_FILENAME;
import static io.scalecube.services.auth.Principal.NULL_PRINCIPAL;

import io.scalecube.services.auth.Principal;
import io.scalecube.services.exceptions.ForbiddenException;
import io.scalecube.services.methods.MethodInfo;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
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
 * Request-scoped context that wraps Reactor {@link Context} and provides typed accessors for
 * request metadata (headers, principal, method info, etc.).
 */
public class RequestContext implements Context {

  private static final Logger LOGGER = LoggerFactory.getLogger(RequestContext.class);

  private static final Object HEADERS_KEY = new Object();
  private static final Object REQUEST_KEY = new Object();
  private static final Object PRINCIPAL_KEY = new Object();
  private static final Object METHOD_INFO_KEY = new Object();
  private static final Object PATH_VARS_KEY = new Object();

  private final Context source;

  /** Creates new {@code RequestContext} with empty base context. */
  public RequestContext() {
    this(Context.empty());
  }

  /**
   * Creates new {@code RequestContext} based on existing context.
   *
   * @param source source context to wrap
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
    return source.getOrDefault(HEADERS_KEY, Collections.emptyMap());
  }

  /**
   * Puts request headers to the context.
   *
   * @param headers headers
   * @return new {@code RequestContext} instance with updated headers
   */
  public RequestContext headers(Map<String, String> headers) {
    return put(HEADERS_KEY, headers);
  }

  /**
   * Returns request.
   *
   * @return request, or {@code null} if not set
   */
  public Object request() {
    return source.getOrDefault(REQUEST_KEY, null);
  }

  /**
   * Puts request to the context.
   *
   * @param request request
   * @return new {@code RequestContext} instance with updated request
   */
  public RequestContext request(Object request) {
    return put(REQUEST_KEY, request);
  }

  /**
   * Returns specific request header by name.
   *
   * @param name header name
   * @return header value, or {@code null} if not found
   */
  public String header(String name) {
    return headers().get(name);
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
   * Returns upload filename from headers.
   *
   * @return upload filename, or {@code null} if not found
   */
  public String uploadFilename() {
    return header(HEADER_UPLOAD_FILENAME);
  }

  /**
   * Returns principal object (authenticated entity) associated with the request.
   *
   * @return {@link Principal} associated with the request, or {@code null} if not set
   */
  public Principal principal() {
    return getOrDefault(PRINCIPAL_KEY, null);
  }

  /**
   * Puts principal to the context.
   *
   * @param principal principal
   * @return new {@code RequestContext} instance with the updated principal
   */
  public RequestContext principal(Principal principal) {
    return put(PRINCIPAL_KEY, principal);
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
    return getOrDefault(METHOD_INFO_KEY, null);
  }

  /**
   * Puts {@link MethodInfo} associated with the request.
   *
   * @param methodInfo methodInfo
   * @return new {@code RequestContext} instance with the updated {@link MethodInfo}
   */
  public RequestContext methodInfo(MethodInfo methodInfo) {
    return put(METHOD_INFO_KEY, methodInfo);
  }

  /**
   * Returns path variables associated with the request.
   *
   * @return path variables, or {@code null} if not set
   */
  public Map<String, String> pathVars() {
    return source.getOrDefault(PATH_VARS_KEY, Collections.emptyMap());
  }

  /**
   * Puts path variables associated with the request.
   *
   * @return path variables, or {@code null} if not set
   */
  public RequestContext pathVars(Map<String, String> pathVars) {
    return put(PATH_VARS_KEY, pathVars);
  }

  /**
   * Returns specific path variable by name.
   *
   * @param name name of the path variable
   * @return path variable value, or {@code null} if not found
   */
  public String pathVar(String name) {
    return pathVars().get(name);
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

    throw new IllegalArgumentException("Unsupported pathVar type: " + type);
  }

  /**
   * Retrieves {@link RequestContext} from the reactor context, wrapping the existing context if
   * necessary.
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
                    "Insufficient permissions -- "
                        + "request context does not have principal, request context: {}",
                    context);
                throw new ForbiddenException("Insufficient permissions");
              }

              final var principal = context.principal();
              final var methodInfo = context.methodInfo();

              final var role = principal.role();
              final var allowedRoles = methodInfo.allowedRoles();
              if (allowedRoles != null
                  && !allowedRoles.isEmpty()
                  && !allowedRoles.contains(principal.role())) {
                LOGGER.warn(
                    "Insufficient permissions -- "
                        + "principal role '{}' is not allowed, request context: {}",
                    role,
                    context);
                throw new ForbiddenException("Insufficient permissions");
              }

              final var allowedPermissions = methodInfo.allowedPermissions(role);
              if (allowedPermissions != null && !allowedPermissions.isEmpty()) {
                for (var allowedPermission : allowedPermissions) {
                  if (!principal.hasPermission(allowedPermission)) {
                    LOGGER.warn(
                        "Insufficient permissions -- "
                            + "allowed permission '{}' is missing, request context: {}",
                        allowedPermission,
                        context);
                    throw new ForbiddenException("Insufficient permissions");
                  }
                }
              }
            });
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", RequestContext.class.getSimpleName() + "[", "]")
        .add("principal=" + principal())
        .add("methodInfo=" + methodInfo())
        .add("headers=" + mask(headers()))
        .add("pathVars=" + mask(pathVars()))
        .add("sourceKeys=" + source.stream().map(Entry::getKey).toList())
        .toString();
  }
}
