package io.scalecube.services.methods;

import io.scalecube.services.CommunicationMode;
import io.scalecube.services.RequestContext;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.BadRequestException;
import io.scalecube.services.exceptions.ServiceException;
import io.scalecube.services.exceptions.ServiceProviderErrorMapper;
import io.scalecube.services.exceptions.UnauthorizedException;
import io.scalecube.services.transport.api.ServiceMessageDataDecoder;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

public final class ServiceMethodInvoker {

  private final Method method;
  private final Object service;
  private final MethodInfo methodInfo;
  private final ServiceProviderErrorMapper errorMapper;
  private final ServiceMessageDataDecoder dataDecoder;
  private final Logger logger;

  public ServiceMethodInvoker(
      Method method,
      Object service,
      MethodInfo methodInfo,
      ServiceProviderErrorMapper errorMapper,
      ServiceMessageDataDecoder dataDecoder,
      Logger logger) {
    this.method = Objects.requireNonNull(method, "method");
    this.service = Objects.requireNonNull(service, "service");
    this.methodInfo = Objects.requireNonNull(methodInfo, "methodInfo");
    this.errorMapper = Objects.requireNonNull(errorMapper, "errorMapper");
    this.dataDecoder = Objects.requireNonNull(dataDecoder, "dataDecoder");
    this.logger = logger;
  }

  /**
   * Invokes service method with single response.
   *
   * @param message request service message
   * @return mono of service message
   */
  public Mono<ServiceMessage> invokeOne(ServiceMessage message) {
    return auth()
        .flatMap(authData -> invokeOne(message, authData))
        .map(response -> toResponse(response, message.qualifier(), message.dataFormat()))
        .onErrorResume(ex -> Mono.just(errorMapper.toMessage(message.qualifier(), ex)))
        .subscribeOn(methodInfo.scheduler());
  }

  private Mono<?> invokeOne(ServiceMessage message, Object authData) {
    final var request = toRequest(message);
    final var qualifier = message.qualifier();

    return Mono.from(invokeRequest(request))
        .contextWrite(context -> enhanceWithRequestContext(context, message))
        // .contextWrite(context -> enhanceWithAuthContext(context, authData))
        .doOnSuccess(
            response -> {
              if (logger != null && logger.isDebugEnabled()) {
                logger.debug("[{}] request: {}, response: {}", qualifier, request, response);
              }
            })
        .doOnError(
            ex -> {
              if (logger != null) {
                logger.error("[{}][error] request: {}", qualifier, request, ex);
              }
            });
  }

  /**
   * Invokes service method with message stream response.
   *
   * @param message request service message
   * @return flux of service messages
   */
  public Flux<ServiceMessage> invokeMany(ServiceMessage message) {
    if (methodInfo.communicationMode() == CommunicationMode.REQUEST_RESPONSE) {
      return Flux.from(invokeOne(message));
    }
    return auth()
        .flatMapMany(authData -> invokeMany(message, authData))
        .map(response -> toResponse(response, message.qualifier(), message.dataFormat()))
        .onErrorResume(ex -> Flux.just(errorMapper.toMessage(message.qualifier(), ex)))
        .subscribeOn(methodInfo.scheduler());
  }

  private Flux<?> invokeMany(ServiceMessage message, Object authData) {
    final var request = toRequest(message);
    final var qualifier = message.qualifier();

    return Flux.from(invokeRequest(request))
        .contextWrite(context -> enhanceWithRequestContext(context, message))
        // .contextWrite(context -> enhanceWithAuthContext(context, authData))
        .doOnSubscribe(
            s -> {
              if (logger != null && logger.isDebugEnabled()) {
                logger.debug("[{}][subscribe] request: {}", qualifier, request);
              }
            })
        .doOnComplete(
            () -> {
              if (logger != null && logger.isDebugEnabled()) {
                logger.debug("[{}][complete] request: {}", qualifier, request);
              }
            })
        .doOnError(
            ex -> {
              if (logger != null) {
                logger.error("[{}][error] request: {}", qualifier, request, ex);
              }
            });
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
            (first, messages) -> {
              final ServiceMessage message = first.get();
              final var qualifier = message.qualifier();

              return auth()
                  .flatMapMany(authData -> invokeBidirectional(messages, authData))
                  .map(response -> toResponse(response, qualifier, message.dataFormat()))
                  .onErrorResume(
                      throwable -> Flux.just(errorMapper.toMessage(qualifier, throwable)))
                  .subscribeOn(methodInfo.scheduler());
            });
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
      throwable = ex.getCause();
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

  private Mono<Object> auth() {
    return RequestContext.deferContextual()
        .flatMap(
            context -> {
              if (!methodInfo.isSecured()) {
                return Mono.just(RequestContext.NULL_PRINCIPAL);
              }

              //    if (authenticator == null) {
              //      if (context.hasKey(AUTH_CONTEXT_KEY)) {
              //        return Mono.just(context.get(AUTH_CONTEXT_KEY));
              //      } else {
              //        throw new UnauthorizedException("Authentication failed");
              //      }
              //    }

              return authenticator
                  .apply(context.headers())
                  .switchIfEmpty(Mono.just(NULL_AUTH_CONTEXT))
                  .onErrorMap(ServiceMethodInvoker::toUnauthorizedException);
            });
  }

  private static UnauthorizedException toUnauthorizedException(Throwable th) {
    if (th instanceof ServiceException e) {
      return new UnauthorizedException(e.errorCode(), e.getMessage());
    } else {
      return new UnauthorizedException(th);
    }
  }

  //  private Context enhanceWithAuthContext(Context context, Object authData) {
  //    if (authData == NULL_AUTH_CONTEXT || principalMapper == null) {
  //      return context.put(AUTH_CONTEXT_KEY, authData);
  //    } else {
  //      final var principal = principalMapper.apply(authData);
  //      return context.put(AUTH_CONTEXT_KEY, principal != null ? principal : NULL_AUTH_CONTEXT);
  //    }
  //  }

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
    final var request = dataDecoder.apply(message, methodInfo.requestType());

    if (!methodInfo.isRequestTypeVoid()
        && !methodInfo.isRequestTypeServiceMessage()
        && !request.hasData(methodInfo.requestType())) {
      throw new BadRequestException("Wrong request data type");
    }

    return methodInfo.isRequestTypeServiceMessage() ? request : request.data();
  }

  private static ServiceMessage toResponse(Object response, String qualifier, String dataFormat) {
    if (response instanceof ServiceMessage message) {
      final var builder = ServiceMessage.from(message).qualifier(qualifier);
      return dataFormat != null && !dataFormat.equals(message.dataFormat())
          ? builder.dataFormat(dataFormat).build()
          : builder.build();
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
        .add("logger=" + logger)
        .toString();
  }
}
