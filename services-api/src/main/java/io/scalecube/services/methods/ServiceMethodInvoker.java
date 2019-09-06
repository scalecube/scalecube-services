package io.scalecube.services.methods;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.exceptions.BadRequestException;
import io.scalecube.services.exceptions.ServiceException;
import io.scalecube.services.exceptions.ServiceProviderErrorMapper;
import io.scalecube.services.exceptions.UnauthorizedException;
import io.scalecube.services.transport.api.ServiceMessageDataDecoder;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
  private final UnaryOperator<ServiceMessage> requestMapper;
  private final UnaryOperator<ServiceMessage> responseMapper;
  private final BiConsumer<ServiceMessage, Throwable> errorHandler;

  /**
   * Constructs a service method invoker out of real service object instance and method info.
   *
   * @param method method
   * @param service service
   * @param methodInfo method info
   * @param errorMapper error mapper
   * @param dataDecoder data decoder
   * @param authenticator authenticator
   * @param requestMapper request mapper
   * @param responseMapper response mapper
   * @param errorHandler error handler
   */
  public ServiceMethodInvoker(
      Method method,
      Object service,
      MethodInfo methodInfo,
      ServiceProviderErrorMapper errorMapper,
      ServiceMessageDataDecoder dataDecoder,
      Authenticator authenticator,
      UnaryOperator<ServiceMessage> requestMapper,
      UnaryOperator<ServiceMessage> responseMapper,
      BiConsumer<ServiceMessage, Throwable> errorHandler) {
    this.method = method;
    this.service = service;
    this.methodInfo = methodInfo;
    this.errorMapper = errorMapper;
    this.dataDecoder = dataDecoder;
    //noinspection unchecked
    this.authenticator = authenticator;
    this.requestMapper = requestMapper;
    this.responseMapper = responseMapper;
    this.errorHandler = errorHandler;
  }

  /**
   * Invokes service method with single response.
   *
   * @param message request service message
   * @return mono of service message
   */
  public Mono<ServiceMessage> invokeOne(ServiceMessage message) {
    return Mono.just(message)
        .map(this::applyDataDecoder)
        .map(requestMapper)
        .doOnError(throwable -> errorHandler.accept(message, throwable))
        .flatMap(
            message1 ->
                authenticate(message1)
                    .flatMap(principal -> Mono.from(invoke(toRequest(message1), principal)))
                    .map(response -> toResponse(response, message1.headers()))
                    .doOnError(throwable -> errorHandler.accept(message1, throwable)))
        .onErrorResume(throwable -> Mono.just(errorMapper.toMessage(throwable)))
        .map(responseMapper);
  }

  /**
   * Invokes service method with message stream response.
   *
   * @param message request service message
   * @return flux of service messages
   */
  public Flux<ServiceMessage> invokeMany(ServiceMessage message) {
    return Mono.just(message)
        .map(this::applyDataDecoder)
        .map(requestMapper)
        .doOnError(throwable -> errorHandler.accept(message, throwable))
        .flatMapMany(
            message1 ->
                authenticate(message1)
                    .flatMapMany(principal -> Flux.from(invoke(toRequest(message1), principal)))
                    .map(response -> toResponse(response, message1.headers()))
                    .doOnError(throwable -> errorHandler.accept(message1, throwable)))
        .onErrorResume(throwable -> Flux.just(errorMapper.toMessage(throwable)))
        .map(responseMapper);
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
            (first, messages) -> {
              if (!first.hasValue()) {
                return messages;
              }
              ServiceMessage firstRequest = first.get();
              return authenticate(firstRequest)
                  .doOnError(th -> applyRequestReleaser(firstRequest, requestReleaser))
                  .flatMapMany(
                      principal ->
                          messages
                              .map(this::applyDataDecoder)
                              .map(this::toRequest)
                              .transform(requests -> Flux.from(invoke(requests, principal))));
            })
        .map(this::toResponse)
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
    return authenticator.authenticate(message).onErrorMap(this::toUnauthorizedException);
  }

  private UnauthorizedException toUnauthorizedException(Throwable th) {
    if (th instanceof ServiceException) {
      ServiceException e = (ServiceException) th;
      return new UnauthorizedException(e.errorCode(), e.getMessage());
    } else {
      return new UnauthorizedException(th);
    }
  }

  private ServiceMessage applyDataDecoder(ServiceMessage message) {
    return dataDecoder.apply(message, methodInfo.requestType());
  }

  private Object toRequest(ServiceMessage message) {
    if (!methodInfo.isRequestTypeVoid()
        && !methodInfo.isRequestTypeServiceMessage()
        && !message.hasData(methodInfo.requestType())) {

      Optional<?> dataOptional = Optional.ofNullable(message.data());
      Class<?> clazz = dataOptional.map(Object::getClass).orElse(null);
      throw new BadRequestException(
          String.format(
              "Expected service request data of type: %s, but received: %s",
              methodInfo.requestType(), clazz));
    }

    return methodInfo.isRequestTypeServiceMessage() ? message : message.data();
  }

  private ServiceMessage toResponse(Object response) {
    return toResponse(response, Collections.emptyMap());
  }

  private ServiceMessage toResponse(Object response, Map<String, String> headers) {
    if (response instanceof ServiceMessage) {
      // return as is; it's assumed headers are copied inside service method
      return (ServiceMessage) response;
    }
    // wrap response object into ServiceMessage and keep request headers
    return ServiceMessage.builder()
        .qualifier(methodInfo.qualifier())
        .headers(headers)
        .data(response)
        .build();
  }

  private void applyRequestReleaser(ServiceMessage request, Consumer<Object> requestReleaser) {
    if (request.data() != null) {
      requestReleaser.accept(request.data());
    }
  }

  /**
   * Shortened version of {@code toString} method.
   *
   * @return service method invoker as string
   */
  public String asString() {
    return new StringJoiner(", ", ServiceMethodInvoker.class.getSimpleName() + "[", "]")
        .add("methodInfo=" + methodInfo.asString())
        .add(
            "serviceMethod='"
                + service.getClass().getCanonicalName()
                + "."
                + method.getName()
                + "("
                + methodInfo.parameterCount()
                + ")"
                + "'")
        .toString();
  }

  @Override
  public String toString() {
    String classAndMethod = service.getClass().getCanonicalName() + "." + method.getName();
    String args =
        Stream.of(method.getParameters())
            .map(Parameter::getType)
            .map(Class::getSimpleName)
            .collect(Collectors.joining(", ", "(", ")"));
    return classAndMethod + args;
  }
}
