package io.scalecube.services;

import static java.util.Objects.requireNonNull;

import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.DefaultErrorMapper;
import io.scalecube.services.exceptions.ServiceClientErrorMapper;
import io.scalecube.services.exceptions.ServiceUnavailableException;
import io.scalecube.services.methods.MethodInfo;
import io.scalecube.services.methods.ServiceMethodRegistry;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.routing.RoundRobinServiceRouter;
import io.scalecube.services.routing.Router;
import io.scalecube.services.routing.Routers;
import io.scalecube.services.transport.api.Address;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.ReferenceCountUtil;
import io.scalecube.services.transport.api.ServiceMessageCodec;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ServiceCall {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceCall.class);

  public static final ServiceMessage UNEXPECTED_EMPTY_RESPONSE =
      ServiceMessage.error(503, 503, "Unexpected empty response");

  private final ClientTransport transport;
  private final ServiceMethodRegistry methodRegistry;
  private final ServiceRegistry serviceRegistry;
  private Router router = Routers.getRouter(RoundRobinServiceRouter.class);
  private ServiceClientErrorMapper errorMapper = DefaultErrorMapper.INSTANCE;

  /**
   * Creates new local {@link ServiceCall}'s definition.
   *
   * @param methodRegistry methodRegistry to be used by {@link ServiceCall}
   */
  public ServiceCall(ServiceMethodRegistry methodRegistry) {
    this(null, null, methodRegistry);
  }

  /**
   * Creates new {@link ServiceCall}'s definition.
   *
   * @param transport transport to be used by {@link ServiceCall}
   * @param serviceRegistry serviceRegistry to be used by {@link ServiceCall}
   * @param methodRegistry methodRegistry to be used by {@link ServiceCall}
   */
  public ServiceCall(
      ClientTransport transport,
      ServiceRegistry serviceRegistry,
      ServiceMethodRegistry methodRegistry) {
    this.transport = transport;
    this.serviceRegistry = serviceRegistry;
    this.methodRegistry = methodRegistry;
  }

  private ServiceCall(ServiceCall other) {
    this.transport = other.transport;
    this.methodRegistry = other.methodRegistry;
    this.serviceRegistry = other.serviceRegistry;
    this.router = other.router;
    this.errorMapper = other.errorMapper;
  }

  /**
   * Creates new {@link ServiceCall}'s definition with a given router.
   *
   * @param routerType given class of the router.
   * @return new {@link ServiceCall} instance.
   */
  public ServiceCall router(Class<? extends Router> routerType) {
    ServiceCall target = new ServiceCall(this);
    target.router = Routers.getRouter(routerType);
    return target;
  }

  /**
   * Creates new {@link ServiceCall}'s definition with a given router.
   *
   * @param router given.
   * @return new {@link ServiceCall} instance.
   */
  public ServiceCall router(Router router) {
    ServiceCall target = new ServiceCall(this);
    target.router = router;
    return target;
  }

  /**
   * Creates new {@link ServiceCall}'s definition with a given error mapper.
   *
   * @param errorMapper given.
   * @return new {@link ServiceCall} instance.
   */
  public ServiceCall errorMapper(ServiceClientErrorMapper errorMapper) {
    ServiceCall target = new ServiceCall(this);
    target.errorMapper = errorMapper;
    return target;
  }

  /**
   * Issues fire-and-forget request.
   *
   * @param request request message to send.
   * @return mono publisher completing normally or with error.
   */
  public Mono<Void> oneWay(ServiceMessage request) {
    return Mono.defer(() -> requestOne(request, Void.class).then());
  }

  /**
   * Issues fire-and-forget request.
   *
   * @param request request message to send.
   * @param address of remote target service to invoke.
   * @return mono publisher completing normally or with error.
   */
  public Mono<Void> oneWay(ServiceMessage request, Address address) {
    return Mono.defer(() -> requestOne(request, Void.class, address).then());
  }

  /**
   * Issues request-and-reply request.
   *
   * @param request request message to send.
   * @return mono publisher completing with single response message or with error.
   */
  public Mono<ServiceMessage> requestOne(ServiceMessage request) {
    return requestOne(request, null);
  }

  /**
   * Issues request-and-reply request.
   *
   * @param request request message to send.
   * @param responseType type of response.
   * @return mono publisher completing with single response message or with error.
   */
  public Mono<ServiceMessage> requestOne(ServiceMessage request, Type responseType) {
    return Mono.defer(
        () -> {
          String qualifier = request.qualifier();
          if (methodRegistry.containsInvoker(qualifier)) { // local service.
            return methodRegistry
                .getInvoker(request.qualifier())
                .invokeOne(request, ServiceMessageCodec::decodeData)
                .map(this::throwIfError);
          } else {
            return addressLookup(request)
                .flatMap(address -> requestOne(request, responseType, address)); // remote service
          }
        });
  }

  /**
   * Given an address issues request-and-reply request to a remote address.
   *
   * @param request request message to send.
   * @param responseType type of response.
   * @param address of remote target service to invoke.
   * @return mono publisher completing with single response message or with error.
   */
  public Mono<ServiceMessage> requestOne(
      ServiceMessage request, Type responseType, Address address) {
    return Mono.defer(
        () -> {
          requireNonNull(address, "requestOne address parameter is required and must not be null");
          requireNonNull(
              transport,
              "transport is required did you forget to provide transport?");
          return transport
              .create(address)
              .requestResponse(request)
              .map(message -> ServiceMessageCodec.decodeData(message, responseType))
              .map(this::throwIfError);
        });
  }

  /**
   * Issues request to service which returns stream of service messages back.
   *
   * @param request request message to send.
   * @return flux publisher of service responses.
   */
  public Flux<ServiceMessage> requestMany(ServiceMessage request) {
    return requestMany(request, null);
  }

  /**
   * Issues request to service which returns stream of service messages back.
   *
   * @param request request with given headers.
   * @param responseType type of responses.
   * @return flux publisher of service responses.
   */
  public Flux<ServiceMessage> requestMany(ServiceMessage request, Type responseType) {
    return Flux.defer(
        () -> {
          String qualifier = request.qualifier();
          if (methodRegistry.containsInvoker(qualifier)) { // local service.
            return methodRegistry
                .getInvoker(request.qualifier())
                .invokeMany(request, ServiceMessageCodec::decodeData)
                .map(this::throwIfError);
          } else {
            return addressLookup(request)
                .flatMapMany(
                    address -> requestMany(request, responseType, address)); // remote service
          }
        });
  }

  /**
   * Given an address issues request to remote service which returns stream of service messages
   * back.
   *
   * @param request request with given headers.
   * @param responseType type of responses.
   * @param address of remote target service to invoke.
   * @return flux publisher of service responses.
   */
  public Flux<ServiceMessage> requestMany(
      ServiceMessage request, Type responseType, Address address) {
    return Flux.defer(
        () -> {
          requireNonNull(address, "requestMany address parameter is required and must not be null");
          requireNonNull(transport, "transport is required and must not be null");
          return transport
              .create(address)
              .requestStream(request)
              .map(message -> ServiceMessageCodec.decodeData(message, responseType))
              .map(this::throwIfError);
        });
  }

  /**
   * Issues stream of service requests to service which returns stream of service messages back.
   *
   * @param publisher of service requests.
   * @return flux publisher of service responses.
   */
  public Flux<ServiceMessage> requestBidirectional(Publisher<ServiceMessage> publisher) {
    return requestBidirectional(publisher, null);
  }

  /**
   * Issues stream of service requests to service which returns stream of service messages back.
   *
   * @param publisher of service requests.
   * @param responseType type of responses.
   * @return flux publisher of service responses.
   */
  public Flux<ServiceMessage> requestBidirectional(
      Publisher<ServiceMessage> publisher, Type responseType) {
    return Flux.from(publisher)
        .switchOnFirst(
            (first, messages) -> {
              if (first.hasValue()) {
                ServiceMessage request = first.get();
                String qualifier = request.qualifier();

                if (methodRegistry.containsInvoker(qualifier)) { // local service.
                  return methodRegistry
                      .getInvoker(qualifier)
                      .invokeBidirectional(messages, ServiceMessageCodec::decodeData)
                      .map(this::throwIfError);
                } else {
                  // remote service
                  return addressLookup(request)
                      .flatMapMany(
                          address -> requestBidirectional(messages, responseType, address));
                }
              }

              return messages;
            });
  }

  /**
   * Given an address issues stream of service requests to service which returns stream of service
   * messages back.
   *
   * @param publisher of service requests.
   * @param responseType type of responses.
   * @param address of remote target service to invoke.
   * @return flux publisher of service responses.
   */
  public Flux<ServiceMessage> requestBidirectional(
      Publisher<ServiceMessage> publisher, Type responseType, Address address) {
    return Flux.defer(
        () -> {
          requireNonNull(
              address, "requestBidirectional address parameter is required and must not be null");
          requireNonNull(transport, "transport is required and must not be null");
          return transport
              .create(address)
              .requestChannel(publisher)
              .map(message -> ServiceMessageCodec.decodeData(message, responseType))
              .map(this::throwIfError);
        });
  }

  /**
   * Create proxy creates a java generic proxy instance by a given service interface.
   *
   * @param serviceInterface Service Interface type.
   * @return newly created service proxy object.
   */
  @SuppressWarnings("unchecked")
  public <T> T api(Class<T> serviceInterface) {

    final ServiceCall serviceCall = this;
    final Map<Method, MethodInfo> genericReturnTypes = Reflect.methodsInfo(serviceInterface);

    // noinspection unchecked
    return (T)
        Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class[] {serviceInterface},
            (proxy, method, params) -> {
              final MethodInfo methodInfo = genericReturnTypes.get(method);
              final Type returnType = methodInfo.parameterizedReturnType();
              final boolean isServiceMessage = methodInfo.isRequestTypeServiceMessage();

              Optional<Object> check =
                  toStringOrEqualsOrHashCode(method.getName(), serviceInterface, params);
              if (check.isPresent()) {
                return check.get(); // toString, hashCode was invoked.
              }

              switch (methodInfo.communicationMode()) {
                case FIRE_AND_FORGET:
                  return serviceCall.oneWay(toServiceMessage(methodInfo, params));

                case REQUEST_RESPONSE:
                  return serviceCall
                      .requestOne(toServiceMessage(methodInfo, params), returnType)
                      .transform(asMono(isServiceMessage));

                case REQUEST_STREAM:
                  return serviceCall
                      .requestMany(toServiceMessage(methodInfo, params), returnType)
                      .transform(asFlux(isServiceMessage));

                case REQUEST_CHANNEL:
                  // this is REQUEST_CHANNEL so it means params[0] must be a publisher - its safe to
                  // cast.
                  return serviceCall
                      .requestBidirectional(
                          Flux.from((Publisher) params[0])
                              .map(data -> toServiceMessage(methodInfo, data)),
                          returnType)
                      .transform(asFlux(isServiceMessage));

                default:
                  throw new IllegalArgumentException(
                      "Communication mode is not supported: " + method);
              }
            });
  }

  private Mono<Address> addressLookup(ServiceMessage request) {
    Callable<Address> callable =
        () -> {
          requireNonNull(serviceRegistry, "serviceRegistry is required and must not be null");
          return router
              .route(serviceRegistry, request)
              .map(ServiceReference::address)
              .orElseThrow(() -> noReachableMemberException(request));
        };
    return Mono.fromCallable(callable)
        .doOnError(
            t -> {
              Optional<Object> dataOptional = Optional.ofNullable(request.data());
              dataOptional.ifPresent(ReferenceCountUtil::safestRelease);
            });
  }

  private ServiceMessage toServiceMessage(MethodInfo methodInfo, Object... params) {
    if (methodInfo.parameterCount() != 0 && params[0] instanceof ServiceMessage) {
      return ServiceMessage.from((ServiceMessage) params[0])
          .qualifier(methodInfo.serviceName(), methodInfo.methodName())
          .build();
    }
    return ServiceMessage.builder()
        .qualifier(methodInfo.serviceName(), methodInfo.methodName())
        .data(methodInfo.parameterCount() != 0 ? params[0] : null)
        .build();
  }

  private ServiceUnavailableException noReachableMemberException(ServiceMessage request) {
    LOGGER.error(
        "Failed  to invoke service, "
            + "No reachable member with such service definition [{}], args [{}]",
        request.qualifier(),
        request);
    return new ServiceUnavailableException(
        "No reachable member with such service: " + request.qualifier());
  }

  /**
   * check and handle toString or equals or hashcode method where invoked.
   *
   * @param method that was invoked.
   * @param serviceInterface for a given service interface.
   * @param args parameters that where invoked.
   * @return Optional object as result of to string equals or hashCode result or absent if none of
   *     these where invoked.
   */
  private Optional<Object> toStringOrEqualsOrHashCode(
      String method, Class<?> serviceInterface, Object... args) {

    switch (method) {
      case "toString":
        return Optional.of(serviceInterface.toString());
      case "equals":
        return Optional.of(serviceInterface.equals(args[0]));
      case "hashCode":
        return Optional.of(serviceInterface.hashCode());

      default:
        return Optional.empty();
    }
  }

  private Function<Flux<ServiceMessage>, Flux<Object>> asFlux(boolean isRequestTypeServiceMessage) {
    return flux -> isRequestTypeServiceMessage ? flux.cast(Object.class) : flux.map(msgToResp());
  }

  private Function<Mono<ServiceMessage>, Mono<Object>> asMono(boolean isRequestTypeServiceMessage) {
    return mono -> isRequestTypeServiceMessage ? mono.cast(Object.class) : mono.map(msgToResp());
  }

  private Function<ServiceMessage, Object> msgToResp() {
    return sm -> sm.hasData() ? sm.data() : UNEXPECTED_EMPTY_RESPONSE;
  }

  private ServiceMessage throwIfError(ServiceMessage message) {
    if (message.isError() && message.hasData(ErrorData.class)) {
      throw Exceptions.propagate(errorMapper.toError(message));
    }

    return message;
  }
}
