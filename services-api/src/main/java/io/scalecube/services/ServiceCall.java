package io.scalecube.services;

import static java.util.Objects.requireNonNull;

import io.scalecube.net.Address;
import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.DefaultErrorMapper;
import io.scalecube.services.exceptions.ServiceClientErrorMapper;
import io.scalecube.services.exceptions.ServiceUnavailableException;
import io.scalecube.services.methods.MethodInfo;
import io.scalecube.services.methods.ServiceMethodInvoker;
import io.scalecube.services.methods.ServiceMethodRegistry;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.routing.Router;
import io.scalecube.services.routing.Routers;
import io.scalecube.services.transport.api.ClientTransport;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ServiceCall {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceCall.class);

  private ClientTransport transport;
  private ServiceMethodRegistry methodRegistry;
  private ServiceRegistry serviceRegistry;
  private Router router;
  private ServiceClientErrorMapper errorMapper = DefaultErrorMapper.INSTANCE;
  private Map<String, String> credentials = Collections.emptyMap();
  private String contentType = ServiceMessage.DEFAULT_DATA_FORMAT;

  public ServiceCall() {}

  private ServiceCall(ServiceCall other) {
    this.transport = other.transport;
    this.methodRegistry = other.methodRegistry;
    this.serviceRegistry = other.serviceRegistry;
    this.router = other.router;
    this.errorMapper = other.errorMapper;
    this.contentType = other.contentType;
    this.credentials = Collections.unmodifiableMap(new HashMap<>(other.credentials));
  }

  /**
   * Setter for {@code clientTransport}.
   *
   * @param clientTransport client transport.
   * @return new {@link ServiceCall} instance.
   */
  public ServiceCall transport(ClientTransport clientTransport) {
    ServiceCall target = new ServiceCall(this);
    target.transport = clientTransport;
    return target;
  }

  /**
   * Setter for {@code serviceRegistry}.
   *
   * @param serviceRegistry service registry.
   * @return new {@link ServiceCall} instance.
   */
  public ServiceCall serviceRegistry(ServiceRegistry serviceRegistry) {
    ServiceCall target = new ServiceCall(this);
    target.serviceRegistry = serviceRegistry;
    return target;
  }

  /**
   * Setter for {@code methodRegistry}.
   *
   * @param methodRegistry method registry.
   * @return new {@link ServiceCall} instance.
   */
  public ServiceCall methodRegistry(ServiceMethodRegistry methodRegistry) {
    ServiceCall target = new ServiceCall(this);
    target.methodRegistry = methodRegistry;
    return target;
  }

  /**
   * Setter for {@code routerType}.
   *
   * @param routerType method registry.
   * @return new {@link ServiceCall} instance.
   */
  public ServiceCall router(Class<? extends Router> routerType) {
    ServiceCall target = new ServiceCall(this);
    target.router = Routers.getRouter(routerType);
    return target;
  }

  /**
   * Setter for {@code router}.
   *
   * @param router router.
   * @return new {@link ServiceCall} instance.
   */
  public ServiceCall router(Router router) {
    ServiceCall target = new ServiceCall(this);
    target.router = router;
    return target;
  }

  /**
   * Setter for {@code errorMapper}.
   *
   * @param errorMapper error mapper.
   * @return new {@link ServiceCall} instance.
   */
  public ServiceCall errorMapper(ServiceClientErrorMapper errorMapper) {
    ServiceCall target = new ServiceCall(this);
    target.errorMapper = errorMapper;
    return target;
  }

  /**
   * Setter for {@code credentials}.
   *
   * @param credentials credentials.
   * @return new {@link ServiceCall} instance.
   */
  public ServiceCall credentials(Map<String, String> credentials) {
    ServiceCall target = new ServiceCall(this);
    target.credentials = Collections.unmodifiableMap(new HashMap<>(credentials));
    return target;
  }

  /**
   * Setter for {@code contentType}.
   *
   * @param contentType content type.
   * @return new {@link ServiceCall} instance.
   */
  public ServiceCall contentType(String contentType) {
    ServiceCall target = new ServiceCall(this);
    target.contentType = contentType;
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
          Objects.requireNonNull(request.qualifier(), "qualifier");

          ServiceMethodInvoker methodInvoker;
          if (methodRegistry != null
              && (methodInvoker = methodRegistry.getInvoker(request.qualifier())) != null) {
            // local service
            return methodInvoker.invokeOne(request).map(this::throwIfError);
          } else {
            // remote service
            return Mono.fromCallable(() -> addressLookup(request))
                .flatMap(address -> requestOne(request, responseType, address));
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
          requireNonNull(transport, "transport is required and must not be null");
          return transport
              .create(address)
              .requestResponse(request, responseType)
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
          Objects.requireNonNull(request.qualifier(), "qualifier");

          ServiceMethodInvoker methodInvoker;
          if (methodRegistry != null
              && (methodInvoker = methodRegistry.getInvoker(request.qualifier())) != null) {
            // local service
            return methodInvoker.invokeMany(request).map(this::throwIfError);
          } else {
            // remote service
            return Mono.fromCallable(() -> addressLookup(request))
                .flatMapMany(address -> requestMany(request, responseType, address));
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
              .requestStream(request, responseType)
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
                Objects.requireNonNull(request.qualifier(), "qualifier");

                ServiceMethodInvoker methodInvoker;
                if (methodRegistry != null
                    && (methodInvoker = methodRegistry.getInvoker(request.qualifier())) != null) {
                  // local service
                  return methodInvoker.invokeBidirectional(messages).map(this::throwIfError);
                } else {
                  // remote service
                  return Mono.fromCallable(() -> addressLookup(request))
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
              .requestChannel(publisher, responseType)
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

    // noinspection unchecked,Convert2Lambda
    return (T)
        Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class[] {serviceInterface, RemoteService.class},
            new InvocationHandler() {
              @Override
              public Object invoke(Object proxy, Method method, Object[] params) {
                Optional<Object> check =
                    toStringOrEqualsOrHashCode(method.getName(), serviceInterface, params);
                if (check.isPresent()) {
                  return check.get(); // toString, hashCode was invoked.
                }

                final MethodInfo methodInfo = genericReturnTypes.get(method);
                final Type returnType = methodInfo.parameterizedReturnType();
                final boolean isServiceMessage = methodInfo.isReturnTypeServiceMessage();

                Object request = methodInfo.requestType() == Void.TYPE ? null : params[0];

                switch (methodInfo.communicationMode()) {
                  case FIRE_AND_FORGET:
                    return serviceCall.oneWay(toServiceMessage(methodInfo, request));

                  case REQUEST_RESPONSE:
                    return serviceCall
                        .requestOne(toServiceMessage(methodInfo, request), returnType)
                        .transform(asMono(isServiceMessage));

                  case REQUEST_STREAM:
                    return serviceCall
                        .requestMany(toServiceMessage(methodInfo, request), returnType)
                        .transform(asFlux(isServiceMessage));

                  case REQUEST_CHANNEL:
                    // this is REQUEST_CHANNEL so it means params[0] must
                    // be a publisher - its safe to cast.
                    //noinspection rawtypes
                    return serviceCall
                        .requestBidirectional(
                            Flux.from((Publisher) request)
                                .map(data -> toServiceMessage(methodInfo, data)),
                            returnType)
                        .transform(asFlux(isServiceMessage));

                  default:
                    throw new IllegalArgumentException(
                        "Communication mode is not supported: " + method);
                }
              }
            });
  }

  private Address addressLookup(ServiceMessage request) {
    return router
        .route(serviceRegistry, request)
        .map(ServiceReference::address)
        .orElseThrow(() -> noReachableMemberException(request));
  }

  private ServiceMessage toServiceMessage(MethodInfo methodInfo, Object request) {
    if (request instanceof ServiceMessage) {
      return ServiceMessage.from((ServiceMessage) request)
          .qualifier(methodInfo.serviceName(), methodInfo.methodName())
          .headers(credentials)
          .dataFormatIfAbsent(contentType)
          .build();
    }

    return ServiceMessage.builder()
        .qualifier(methodInfo.serviceName(), methodInfo.methodName())
        .headers(credentials)
        .data(request)
        .dataFormatIfAbsent(contentType)
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

  private Function<Flux<ServiceMessage>, Flux<Object>> asFlux(boolean isReturnTypeServiceMessage) {
    return flux ->
        isReturnTypeServiceMessage ? flux.cast(Object.class) : flux.map(ServiceMessage::data);
  }

  private Function<Mono<ServiceMessage>, Mono<Object>> asMono(boolean isReturnTypeServiceMessage) {
    return mono ->
        isReturnTypeServiceMessage ? mono.cast(Object.class) : mono.map(ServiceMessage::data);
  }

  private ServiceMessage throwIfError(ServiceMessage message) {
    if (message.isError() && message.hasData(ErrorData.class)) {
      throw Exceptions.propagate(errorMapper.toError(message));
    }
    return message;
  }
}
