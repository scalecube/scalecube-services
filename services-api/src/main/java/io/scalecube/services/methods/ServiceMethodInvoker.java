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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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

  /**
   * Constructs a service method invoker out of real service object instance and method info.
   *
   * @param method service method (required)
   * @param service service instance (required)
   * @param methodInfo method information (required)
   * @param errorMapper error mapper (required)
   * @param dataDecoder data decoder (required)
   * @param authenticator authenticator (optional)
   * @param principalMapper principal mapper (optional)
   */
  public ServiceMethodInvoker(
      Method method,
      Object service,
      MethodInfo methodInfo,
      ServiceProviderErrorMapper errorMapper,
      ServiceMessageDataDecoder dataDecoder,
      Authenticator<Object> authenticator,
      PrincipalMapper<Object, Object> principalMapper) {
    this.method = Objects.requireNonNull(method, "method");
    this.service = Objects.requireNonNull(service, "service");
    this.methodInfo = Objects.requireNonNull(methodInfo, "methodInfo");
    this.errorMapper = Objects.requireNonNull(errorMapper, "errorMapper");
    this.dataDecoder = Objects.requireNonNull(dataDecoder, "dataDecoder");
    this.authenticator = authenticator;
    this.principalMapper = principalMapper;
  }

  /**
   * Invokes service method with single response.
   *
   * @param message request service message
   * @return mono of service message
   */
  public Mono<ServiceMessage> invokeOne(ServiceMessage message) {
    return Mono.deferContextual(context -> authenticate(message, (Context) context))
        .flatMap(authData -> deferWithContextOne(message, authData))
        .map(response -> toResponse(response, message.qualifier(), message.dataFormat()))
        .onErrorResume(
            throwable -> Mono.just(errorMapper.toMessage(message.qualifier(), throwable)))
        .subscribeOn(methodInfo.scheduler());
  }

  /**
   * Invokes service method with message stream response.
   *
   * @param message request service message
   * @return flux of service messages
   */
  public Flux<ServiceMessage> invokeMany(ServiceMessage message) {
    return Mono.deferContextual(context -> authenticate(message, (Context) context))
        .flatMapMany(authData -> deferWithContextMany(message, authData))
        .map(response -> toResponse(response, message.qualifier(), message.dataFormat()))
        .onErrorResume(
            throwable -> Flux.just(errorMapper.toMessage(message.qualifier(), throwable)))
        .subscribeOn(methodInfo.scheduler());
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
                    .flatMapMany(authData -> deferWithContextBidirectional(messages, authData))
                    .map(
                        response ->
                            toResponse(response, first.get().qualifier(), first.get().dataFormat()))
                    .onErrorResume(
                        throwable ->
                            Flux.just(errorMapper.toMessage(first.get().qualifier(), throwable)))
                    .subscribeOn(methodInfo.scheduler()));
  }

  private Mono<?> deferWithContextOne(ServiceMessage message, Object authData) {
    return Mono.deferContextual(context -> Mono.from(invoke(toRequest(message))))
        .contextWrite(context -> enhanceContext(authData, context));
  }

  private Flux<?> deferWithContextMany(ServiceMessage message, Object authData) {
    return Flux.deferContextual(context -> Flux.from(invoke(toRequest(message))))
        .contextWrite(context -> enhanceContext(authData, context));
  }

  private Flux<?> deferWithContextBidirectional(Flux<ServiceMessage> messages, Object authData) {
    return Flux.deferContextual(context -> messages.map(this::toRequest).transform(this::invoke))
        .contextWrite(context -> enhanceContext(authData, context));
  }

  private Publisher<?> invoke(Object request) {
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
        .onErrorMap(this::toUnauthorizedException);
  }

  private UnauthorizedException toUnauthorizedException(Throwable th) {
    if (th instanceof ServiceException) {
      ServiceException e = (ServiceException) th;
      return new UnauthorizedException(e.errorCode(), e.getMessage());
    } else {
      return new UnauthorizedException(th);
    }
  }

  private Context enhanceContext(Object authData, Context context) {
    if (authData == NULL_AUTH_CONTEXT || principalMapper == null) {
      return context.put(AUTH_CONTEXT_KEY, authData);
    }

    Object mappedData = principalMapper.apply(authData);

    return context.put(AUTH_CONTEXT_KEY, mappedData != null ? mappedData : NULL_AUTH_CONTEXT);
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

  private ServiceMessage toResponse(Object response, String qualifier, String dataFormat) {
    if (response instanceof ServiceMessage) {
      ServiceMessage message = (ServiceMessage) response;
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
        .toString();
  }
}
