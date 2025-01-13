package io.scalecube.services.methods;

import static io.scalecube.services.auth.Authenticator.AUTH_CONTEXT_KEY;
import static io.scalecube.services.auth.Authenticator.NULL_AUTH_CONTEXT;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.auth.PrincipalMapper;
import io.scalecube.services.exceptions.BadRequestException;
import io.scalecube.services.exceptions.ServiceException;
import io.scalecube.services.exceptions.ServiceProviderErrorMapper;
import io.scalecube.services.exceptions.UnauthorizedException;
import io.scalecube.services.transport.api.ServiceMessageDataDecoder;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

public final class ServiceMethodInvoker {

  private final Method method;
  private final Object service;
  private final MethodInfo methodInfo;
  private final ServiceProviderErrorMapper errorMapper;
  private final ServiceMessageDataDecoder dataDecoder;
  private final Authenticator<Object> authenticator;
  private final PrincipalMapper<Object, Object> principalMapper;
  private final Logger logger;
  private final Level level;

  public ServiceMethodInvoker(
      Method method,
      Object service,
      MethodInfo methodInfo,
      ServiceProviderErrorMapper errorMapper,
      ServiceMessageDataDecoder dataDecoder,
      Authenticator<Object> authenticator,
      PrincipalMapper<Object, Object> principalMapper,
      Logger logger,
      Level level) {
    this.method = Objects.requireNonNull(method, "method");
    this.service = Objects.requireNonNull(service, "service");
    this.methodInfo = Objects.requireNonNull(methodInfo, "methodInfo");
    this.errorMapper = Objects.requireNonNull(errorMapper, "errorMapper");
    this.dataDecoder = Objects.requireNonNull(dataDecoder, "dataDecoder");
    this.authenticator = authenticator;
    this.principalMapper = principalMapper;
    this.logger = logger;
    this.level = level;
  }

  /**
   * Invokes service method with single response.
   *
   * @param message request service message
   * @return mono of service message
   */
  public Mono<ServiceMessage> invokeOne(ServiceMessage message) {
    return Mono.deferContextual(context -> authenticate(message, (Context) context))
        .flatMap(authData -> invokeOne(message, authData))
        .map(response -> toResponse(response, message.qualifier(), message.dataFormat()))
        .onErrorResume(
            throwable -> Mono.just(errorMapper.toMessage(message.qualifier(), throwable)))
        .subscribeOn(methodInfo.scheduler());
  }

  private Mono<?> invokeOne(ServiceMessage message, Object authData) {
    return Mono.deferContextual(
            context -> {
              final var request = toRequest(message);
              final var qualifier = message.qualifier();
              return Mono.from(invokeRequest(request))
                  .doOnSuccess(
                      response -> {
                        if (logger != null && logger.isLoggable(level)) {
                          logger.log(
                              level,
                              "[{0}] request: " + request + ", response: " + response,
                              qualifier);
                        }
                      })
                  .doOnError(
                      ex -> {
                        if (logger != null) {
                          logger.log(Level.ERROR, "[{0}] request: " + request, qualifier, ex);
                        }
                      });
            })
        .contextWrite(context -> enhanceWithRequestContext(context, message))
        .contextWrite(context -> enhanceWithAuthContext(context, authData));
  }

  /**
   * Invokes service method with message stream response.
   *
   * @param message request service message
   * @return flux of service messages
   */
  public Flux<ServiceMessage> invokeMany(ServiceMessage message) {
    return Mono.deferContextual(context -> authenticate(message, (Context) context))
        .flatMapMany(authData -> invokeMany(message, authData))
        .map(response -> toResponse(response, message.qualifier(), message.dataFormat()))
        .onErrorResume(
            throwable -> Flux.just(errorMapper.toMessage(message.qualifier(), throwable)))
        .subscribeOn(methodInfo.scheduler());
  }

  private Flux<?> invokeMany(ServiceMessage message, Object authData) {
    return Flux.deferContextual(
            context -> {
              final var request = toRequest(message);
              final var qualifier = message.qualifier();
              return Flux.from(invokeRequest(request))
                  .doOnSubscribe(
                      s -> {
                        if (logger != null && logger.isLoggable(level)) {
                          logger.log(level, "[{0}] request: " + request, qualifier);
                        }
                      })
                  .doOnError(
                      ex -> {
                        if (logger != null) {
                          logger.log(Level.ERROR, "[{0}] request: " + request, qualifier, ex);
                        }
                      });
            })
        .contextWrite(context -> enhanceWithRequestContext(context, message))
        .contextWrite(context -> enhanceWithAuthContext(context, authData));
  }

  /**
   * Invokes service method with bidirectional communication.
   *
   * @param publisher request service message
   * @return flux of service messages
   */
  public Flux<ServiceMessage> invokeBidirectional(Publisher<ServiceMessage> publisher) {
    return Flux.from(publisher)
        .switchOnFirst(
            (first, messages) ->
                Mono.deferContextual(context -> authenticate(first.get(), (Context) context))
                    .flatMapMany(authData -> invokeBidirectional(messages, authData))
                    .map(
                        response ->
                            toResponse(response, first.get().qualifier(), first.get().dataFormat()))
                    .onErrorResume(
                        throwable ->
                            Flux.just(errorMapper.toMessage(first.get().qualifier(), throwable)))
                    .subscribeOn(methodInfo.scheduler()));
  }

  private Flux<?> invokeBidirectional(Flux<ServiceMessage> messages, Object authData) {
    return Flux.deferContextual(
            context -> messages.map(this::toRequest).transform(this::invokeRequest))
        .contextWrite(context -> enhanceWithAuthContext(context, authData));
  }

  private Publisher<?> invokeRequest(Object request) {
    Publisher<?> result = null;
    Throwable throwable = null;
    try {
      if (methodInfo.parameterCount() == 0) {
        result = (Publisher<?>) method.invoke(service);
      } else {
        Object[] arguments = prepareArguments(request);
        result = (Publisher<?>) method.invoke(service, arguments);
      }
      if (result == null) {
        result = Mono.empty();
      }
    } catch (InvocationTargetException ex) {
      throwable = Optional.ofNullable(ex.getCause()).orElse(ex);
    } catch (Throwable ex) {
      throwable = ex;
    }
    return throwable != null ? Mono.error(throwable) : result;
  }

  private Object[] prepareArguments(Object request) {
    Object[] arguments = new Object[methodInfo.parameterCount()];
    if (methodInfo.requestType() != Void.TYPE) {
      arguments[0] = request;
    }
    return arguments;
  }

  private Mono<Object> authenticate(ServiceMessage message, Context context) {
    if (!methodInfo.isSecured()) {
      return Mono.just(NULL_AUTH_CONTEXT);
    }

    if (authenticator == null) {
      if (context.hasKey(AUTH_CONTEXT_KEY)) {
        return Mono.just(context.get(AUTH_CONTEXT_KEY));
      } else {
        throw new UnauthorizedException("Authentication failed");
      }
    }

    return authenticator
        .apply(message.headers())
        .switchIfEmpty(Mono.just(NULL_AUTH_CONTEXT))
        .onErrorMap(ServiceMethodInvoker::toUnauthorizedException);
  }

  private static UnauthorizedException toUnauthorizedException(Throwable th) {
    if (th instanceof ServiceException e) {
      return new UnauthorizedException(e.errorCode(), e.getMessage());
    } else {
      return new UnauthorizedException(th);
    }
  }

  private Context enhanceWithAuthContext(Context context, Object authData) {
    if (authData == NULL_AUTH_CONTEXT || principalMapper == null) {
      return context.put(AUTH_CONTEXT_KEY, authData);
    } else {
      final var principal = principalMapper.apply(authData);
      return context.put(AUTH_CONTEXT_KEY, principal != null ? principal : NULL_AUTH_CONTEXT);
    }
  }

  private Context enhanceWithRequestContext(Context context, ServiceMessage message) {
    final var headers = message.headers();
    final var principal = context.get(AUTH_CONTEXT_KEY);
    final var dynamicQualifier = methodInfo.dynamicQualifier();

    Map<String, String> pathVars = null;
    if (dynamicQualifier != null) {
      pathVars = dynamicQualifier.matchQualifier(message.qualifier());
    }

    return context.put(RequestContext.class, new RequestContext(headers, principal, pathVars));
  }

  private Object toRequest(ServiceMessage message) {
    ServiceMessage request = dataDecoder.apply(message, methodInfo.requestType());

    if (!methodInfo.isRequestTypeVoid()
        && !methodInfo.isRequestTypeServiceMessage()
        && !request.hasData(methodInfo.requestType())) {

      Optional<?> dataOptional = Optional.ofNullable(request.data());
      Class<?> clazz = dataOptional.map(Object::getClass).orElse(null);
      throw new BadRequestException(
          String.format(
              "Expected service request data of type: %s, but received: %s",
              methodInfo.requestType(), clazz));
    }

    return methodInfo.isRequestTypeServiceMessage() ? request : request.data();
  }

  private static ServiceMessage toResponse(Object response, String qualifier, String dataFormat) {
    if (response instanceof ServiceMessage message) {
      if (dataFormat != null && !dataFormat.equals(message.dataFormat())) {
        return ServiceMessage.from(message).qualifier(qualifier).dataFormat(dataFormat).build();
      }
      return ServiceMessage.from(message).qualifier(qualifier).build();
    }
    return ServiceMessage.builder()
        .qualifier(qualifier)
        .data(response)
        .dataFormatIfAbsent(dataFormat)
        .build();
  }

  public Object service() {
    return service;
  }

  public MethodInfo methodInfo() {
    return methodInfo;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ServiceMethodInvoker.class.getSimpleName() + "[", "]")
        .add("method=" + method)
        .add("service=" + service)
        .add("methodInfo=" + methodInfo)
        .add("errorMapper=" + errorMapper)
        .add("dataDecoder=" + dataDecoder)
        .add("authenticator=" + authenticator)
        .add("principalMapper=" + principalMapper)
        .add("logger=" + logger)
        .add("level=" + level)
        .toString();
  }
}
