package io.scalecube.services.methods;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.auth.AuthContextRegistry;
import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.exceptions.BadRequestException;
import io.scalecube.services.exceptions.ServiceException;
import io.scalecube.services.exceptions.ServiceProviderErrorMapper;
import io.scalecube.services.exceptions.UnauthorizedException;
import io.scalecube.services.transport.api.ServiceMessageDataDecoder;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.function.Consumer;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Invoker of service method. Prepares service message request before call as well as doing some
 * handling of a product of the service call.
 */
public final class ServiceMethodInvoker {

  private static final Object NO_PRINCIPAL = new Object();

  private final Method method;
  private final Object service;
  private final MethodInfo methodInfo;
  private final ServiceProviderErrorMapper errorMapper;
  private final ServiceMessageDataDecoder dataDecoder;
  private final Authenticator<Object> authenticator;
  private final AuthContextRegistry authContextRegistry;

  /**
   * Constructs a service method invoker out of real service object instance and method info.
   *
   * @param method service method
   * @param service service instance
   * @param methodInfo method information
   * @param errorMapper error mapper
   * @param dataDecoder data decoder
   * @param authContextRegistry auth context registry
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public ServiceMethodInvoker(
      Method method,
      Object service,
      MethodInfo methodInfo,
      ServiceProviderErrorMapper errorMapper,
      ServiceMessageDataDecoder dataDecoder,
      Authenticator authenticator,
      AuthContextRegistry authContextRegistry) {
    this.method = method;
    this.service = service;
    this.methodInfo = methodInfo;
    this.errorMapper = errorMapper;
    this.dataDecoder = dataDecoder;
    this.authenticator = authenticator;
    this.authContextRegistry = authContextRegistry;
  }

  /**
   * Invokes service method with single response.
   *
   * @param message request service message
   * @param requestReleaser request releaser
   * @return mono of service message
   */
  public Mono<ServiceMessage> invokeOne(ServiceMessage message, Consumer<Object> requestReleaser) {
    return authenticate(message)
        .doOnError(th -> applyRequestReleaser(message, requestReleaser))
        .flatMap(principal -> Mono.from(invoke(toRequest(message), principal)))
        .map(response -> toResponse(response, message.dataFormat()))
        .onErrorResume(throwable -> Mono.just(errorMapper.toMessage(throwable)));
  }

  /**
   * Invokes service method with message stream response.
   *
   * @param message request service message
   * @param requestReleaser request releaser
   * @return flux of service messages
   */
  public Flux<ServiceMessage> invokeMany(ServiceMessage message, Consumer<Object> requestReleaser) {
    return authenticate(message)
        .doOnError(th -> applyRequestReleaser(message, requestReleaser))
        .flatMapMany(principal -> Flux.from(invoke(toRequest(message), principal)))
        .map(response -> toResponse(response, message.dataFormat()))
        .onErrorResume(throwable -> Flux.just(errorMapper.toMessage(throwable)));
  }

  /**
   * Invokes service method with bidirectional communication.
   *
   * @param publisher request service message
   * @param requestReleaser request releaser
   * @return flux of service messages
   */
  public Flux<ServiceMessage> invokeBidirectional(
      Publisher<ServiceMessage> publisher, Consumer<Object> requestReleaser) {
    return Flux.from(publisher)
        .switchOnFirst(
            (first, messages) ->
                authenticate(first.get())
                    .doOnError(th -> applyRequestReleaser(first.get(), requestReleaser))
                    .flatMapMany(
                        principal ->
                            messages
                                .map(this::toRequest)
                                .transform(request -> invoke(request, principal)))
                    .map(response -> toResponse(response, first.get().dataFormat())))
        .onErrorResume(throwable -> Flux.just(errorMapper.toMessage(throwable)));
  }

  private Publisher<?> invoke(Object request, Object principal) {
    Publisher<?> result = null;
    Throwable throwable = null;
    try {
      if (methodInfo.parameterCount() == 0) {
        result = (Publisher<?>) method.invoke(service);
      } else {
        Object[] arguments = prepareArguments(request, principal);
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

  private Object[] prepareArguments(Object request, Object principal) {
    Object[] arguments = new Object[methodInfo.parameterCount()];
    Object principalArg = principal.equals(NO_PRINCIPAL) ? null : principal;

    if (methodInfo.requestType() != Void.TYPE) {
      arguments[0] = request;
    } else {
      arguments[0] = principalArg;
    }

    if (methodInfo.parameterCount() > 1) {
      arguments[1] = principalArg;
    }
    return arguments;
  }

  private Mono<Object> authenticate(ServiceMessage message) {
    return Mono.defer(() -> authenticate0(message)).defaultIfEmpty(NO_PRINCIPAL);
  }

  private Mono<Object> authenticate0(ServiceMessage message) {
    if (!methodInfo.isAuth()) {
      return Mono.empty();
    }
    if (authenticator == null) {
      throw new UnauthorizedException("Authenticator not found");
    }
    return authenticator
        .authenticate(message, authContextRegistry)
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

  private ServiceMessage toResponse(Object response, String dataFormat) {
    if (response instanceof ServiceMessage) {
      ServiceMessage message = (ServiceMessage) response;
      if (dataFormat != null && !dataFormat.equals(message.dataFormat())) {
        return ServiceMessage.from(message).dataFormat(dataFormat).build();
      }
      return message;
    }
    return ServiceMessage.builder()
        .qualifier(methodInfo.qualifier())
        .data(response)
        .dataFormatIfAbsent(dataFormat)
        .build();
  }

  private void applyRequestReleaser(ServiceMessage request, Consumer<Object> requestReleaser) {
    if (request.data() != null) {
      requestReleaser.accept(request.data());
    }
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
        .toString();
  }
}
