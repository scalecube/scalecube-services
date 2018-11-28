package io.scalecube.services;

import static java.util.Objects.requireNonNull;

import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.Qualifier;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.ExceptionProcessor;
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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ServiceCall {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceCall.class);

  public static final ServiceMessage UNEXPECTED_EMPTY_RESPONSE =
      ServiceMessage.builder()
          .qualifier(Qualifier.asError(503))
          .data(new ErrorData(503, "Unexpected empty response"))
          .build();

  private final ClientTransport transport;
  private final ServiceMethodRegistry methodRegistry;
  private final ServiceRegistry serviceRegistry;
  private final Router router;

  private ServiceCall(Call call) {
    this.transport = call.transport;
    this.methodRegistry = call.methodRegistry;
    this.serviceRegistry = call.serviceRegistry;
    this.router = call.router;
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
  public Mono<ServiceMessage> requestOne(ServiceMessage request, Class<?> responseType) {
    return Mono.defer(
        () -> {
          String qualifier = request.qualifier();
          if (methodRegistry.containsInvoker(qualifier)) { // local service.
            return methodRegistry
                .getInvoker(request.qualifier())
                .invokeOne(request, ServiceMessageCodec::decodeData)
                .onErrorMap(ExceptionProcessor::mapException);
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
      ServiceMessage request, Class<?> responseType, Address address) {
    return Mono.defer(
        () -> {
          requireNonNull(address, "requestOne address paramter is required and must not be null");
          return transport
              .create(address)
              .requestResponse(request)
              .map(message -> ServiceMessageCodec.decodeData(message, responseType));
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
  public Flux<ServiceMessage> requestMany(ServiceMessage request, Class<?> responseType) {
    return Flux.defer(
        () -> {
          String qualifier = request.qualifier();
          if (methodRegistry.containsInvoker(qualifier)) { // local service.
            return methodRegistry
                .getInvoker(request.qualifier())
                .invokeMany(request, ServiceMessageCodec::decodeData)
                .onErrorMap(ExceptionProcessor::mapException);
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
      ServiceMessage request, Class<?> responseType, Address address) {
    return Flux.defer(
        () -> {
          requireNonNull(address, "requestMany address paramter is required and must not be null");
          return transport
              .create(address)
              .requestStream(request)
              .map(message -> ServiceMessageCodec.decodeData(message, responseType));
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
      Publisher<ServiceMessage> publisher, Class<?> responseType) {
    return Flux.from(HeadAndTail.createFrom(publisher))
        .flatMap(
            pair -> {
              ServiceMessage request = pair.head();
              String qualifier = request.qualifier();
              Flux<ServiceMessage> messages = Flux.from(pair.tail()).startWith(request);

              if (methodRegistry.containsInvoker(qualifier)) { // local service.
                return methodRegistry
                    .getInvoker(qualifier)
                    .invokeBidirectional(messages, ServiceMessageCodec::decodeData)
                    .onErrorMap(ExceptionProcessor::mapException);
              } else {
                // remote service
                return addressLookup(request)
                    .flatMapMany(address -> requestBidirectional(messages, responseType, address));
              }
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
      Publisher<ServiceMessage> publisher, Class<?> responseType, Address address) {
    return Flux.defer(
        () -> {
          requireNonNull(
              address, "requestBidirectional address paramter is required and must not be null");
          return transport
              .create(address)
              .requestChannel(publisher)
              .map(message -> ServiceMessageCodec.decodeData(message, responseType));
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
              final Class<?> returnType = methodInfo.parameterizedReturnType();
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
        () ->
            router
                .route(serviceRegistry, request)
                .map(ServiceReference::address)
                .orElseThrow(() -> noReachableMemberException(request));
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

  /**
   * This class represents {@link ServiceCall}'s definition. All {@link ServiceCall} must be created
   * out of this definition.
   */
  public static class Call {

    private final ClientTransport transport;
    private final ServiceMethodRegistry methodRegistry;
    private final ServiceRegistry serviceRegistry;
    private Router router = Routers.getRouter(RoundRobinServiceRouter.class);

    /**
     * Creates new {@link ServiceCall}'s definition.
     *
     * @param transport - transport to be used by {@link ServiceCall} that is created form this
     *     {@link Call}
     * @param methodRegistry - methodRegistry to be used by {@link ServiceCall} that is created form
     *     this {@link Call}
     * @param serviceRegistry - serviceRegistry to be used by {@link ServiceCall} that is created
     *     form this {@link Call}
     */
    public Call(
        ClientTransport transport,
        ServiceMethodRegistry methodRegistry,
        ServiceRegistry serviceRegistry) {
      this.transport = transport;
      this.serviceRegistry = serviceRegistry;
      this.methodRegistry = methodRegistry;
    }

    public Call router(Class<? extends Router> routerType) {
      this.router = Routers.getRouter(routerType);
      return this;
    }

    public Call router(Router router) {
      this.router = router;
      return this;
    }

    public ServiceCall create() {
      return new ServiceCall(this);
    }
  }
}
