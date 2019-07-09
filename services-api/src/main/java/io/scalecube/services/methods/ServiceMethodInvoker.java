package io.scalecube.services.methods;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.auth.Principal;
import io.scalecube.services.exceptions.BadRequestException;
import io.scalecube.services.exceptions.ServiceProviderErrorMapper;
import io.scalecube.services.exceptions.UnauthorizedException;
import io.scalecube.services.transport.api.ServiceMessageDataDecoder;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Optional;
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
  private final Authenticator authenticator;

  /**
   * Constructs a service method invoker out of real service object instance and method info.
   *
   * @param method service method
   * @param service service instance
   * @param methodInfo method information
   * @param errorMapper error mapper
   * @param dataDecoder data decoder
   */
  public ServiceMethodInvoker(
      Method method,
      Object service,
      MethodInfo methodInfo,
      ServiceProviderErrorMapper errorMapper,
      ServiceMessageDataDecoder dataDecoder,
      Authenticator authenticator) {
    this.method = method;
    this.service = service;
    this.methodInfo = methodInfo;
    this.errorMapper = errorMapper;
    this.dataDecoder = dataDecoder;
    this.authenticator = authenticator;
  }

  /**
   * Invokes service method with single response.
   *
   * @param message request service message
   * @return mono of service message
   */
  public Mono<ServiceMessage> invokeOne(ServiceMessage message) {
    return authenticate(message)
        .flatMap(principal -> Mono.from(invoke(toRequest(message), principal)))
        .map(this::toResponse)
        .onErrorResume(throwable -> Mono.just(errorMapper.toMessage(throwable)));
  }

  /**
   * Invokes service method with message stream response.
   *
   * @param message request service message
   * @return flux of service messages
   */
  public Flux<ServiceMessage> invokeMany(ServiceMessage message) {
    return authenticate(message)
        .flatMapMany(principal -> Flux.from(invoke(toRequest(message), principal)))
        .map(this::toResponse)
        .onErrorResume(throwable -> Flux.just(errorMapper.toMessage(throwable)));
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
              if (first.hasValue()) {
                return authenticate(first.get())
                    .flatMapMany(
                        principal ->
                            messages
                                .map(this::toRequest)
                                .transform(request -> invoke(request, principal)))
                    .map(this::toResponse);
              } else {
                return messages
                    .map(this::toRequest)
                    .transform(request -> invoke(request, null))
                    .map(this::toResponse);
              }
            })
        .onErrorResume(throwable -> Flux.just(errorMapper.toMessage(throwable)));
  }

  private Publisher<?> invoke(Object request, Object principal) {
    Publisher<?> result = null;
    Throwable throwable = null;
    try {
      if (method.getParameterCount() == 0) {
        result = (Publisher<?>) method.invoke(service);
      } else {
        Parameter[] parameters = method.getParameters();
        Object[] arguments = new Object[parameters.length];

        for (int i = 0; i < parameters.length; i++) {
          if (parameters[i].isAnnotationPresent(Principal.class)) {
            arguments[i] = principal.equals(NO_PRINCIPAL) ? null : principal;
          } else {
            arguments[i] = request;
          }
        }

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

  @SuppressWarnings("unchecked")
  private Mono<Object> authenticate(ServiceMessage message) {
    return Mono.defer(
        () -> {
          if (!methodInfo.isAuth()) {
            return Mono.empty();
          }
          if (authenticator == null) {
            throw new UnauthorizedException("Authenticator not found");
          }
          return authenticator.authenticate(message);
        })
        .defaultIfEmpty(NO_PRINCIPAL);
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

  private ServiceMessage toResponse(Object response) {
    return (response instanceof ServiceMessage)
        ? (ServiceMessage) response
        : ServiceMessage.builder().qualifier(methodInfo.qualifier()).data(response).build();
  }

  @Override
  public String toString() {
    String classAndMethod = service.getClass().getCanonicalName() + "#" + method.getName();
    String args =
        Stream.of(method.getParameters())
            .map(Parameter::getType)
            .map(Class::getSimpleName)
            .collect(Collectors.joining(", ", "(", ")"));
    return classAndMethod + args;
  }
}
