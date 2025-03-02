package io.scalecube.services;

import static io.scalecube.services.auth.Principal.NULL_PRINCIPAL;

import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.DefaultErrorMapper;
import io.scalecube.services.exceptions.ServiceClientErrorMapper;
import io.scalecube.services.exceptions.ServiceUnavailableException;
import io.scalecube.services.methods.MethodInfo;
import io.scalecube.services.methods.ServiceMethodInvoker;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.routing.Router;
import io.scalecube.services.routing.Routers;
import io.scalecube.services.transport.api.ClientTransport;
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

public class ServiceCall implements AutoCloseable {

  private ClientTransport transport;
  private ServiceRegistry serviceRegistry;
  private Router router;
  private ServiceClientErrorMapper errorMapper = DefaultErrorMapper.INSTANCE;
  private Map<String, String> credentials = Collections.emptyMap();
  private String contentType = ServiceMessage.DEFAULT_DATA_FORMAT;
  private Logger logger;

  public ServiceCall() {}

  private ServiceCall(ServiceCall other) {
    this.transport = other.transport;
    this.serviceRegistry = other.serviceRegistry;
    this.router = other.router;
    this.errorMapper = other.errorMapper;
    this.contentType = other.contentType;
    this.credentials = Collections.unmodifiableMap(new HashMap<>(other.credentials));
    this.logger = other.logger;
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
   * Setter for {@link ServiceCall} {@code logger}.
   *
   * @param name logger name (optional)
   * @return new {@link ServiceCall} instance.
   */
  public ServiceCall logger(String name) {
    ServiceCall target = new ServiceCall(this);
    target.logger = name != null ? LoggerFactory.getLogger(name) : null;
    return target;
  }

  /**
   * Setter for {@link ServiceCall} {@code logger}.
   *
   * @param clazz logger name (optional)
   * @return new {@link ServiceCall} instance.
   */
  public ServiceCall logger(Class<?> clazz) {
    ServiceCall target = new ServiceCall(this);
    target.logger = clazz != null ? LoggerFactory.getLogger(clazz) : null;
    return target;
  }

  /**
   * Setter for {@link ServiceCall} {@code logger}.
   *
   * @param logger logger (optional)
   * @return new {@link ServiceCall} instance.
   */
  public ServiceCall logger(Logger logger) {
    ServiceCall target = new ServiceCall(this);
    target.logger = logger;
    return target;
  }

  /**
   * Invokes fire-and-forget request.
   *
   * @param request request message to send.
   * @return mono publisher completing normally or with error.
   */
  public Mono<Void> oneWay(ServiceMessage request) {
    return requestOne(request, Void.class).then();
  }

  /**
   * Invokes request-and-reply request.
   *
   * @param request request message to send.
   * @return mono publisher completing with single response message or with error.
   */
  public Mono<ServiceMessage> requestOne(ServiceMessage request) {
    return requestOne(request, null);
  }

  /**
   * Invokes request-and-reply request.
   *
   * @param request request message to send.
   * @param responseType type of response (optional).
   * @return mono publisher completing with single response message or with error.
   */
  public Mono<ServiceMessage> requestOne(ServiceMessage request, Type responseType) {
    return Mono.defer(
            () -> {
              ServiceMethodInvoker methodInvoker;
              if (serviceRegistry != null
                  && (methodInvoker = serviceRegistry.lookupInvoker(request)) != null) {
                // local service
                return methodInvoker
                    .invokeOne(request)
                    .map(this::throwIfError)
                    .contextWrite(
                        context -> {
                          if (context.hasKey(RequestContext.class)) {
                            return context;
                          } else {
                            return new RequestContext(context)
                                .headers(request.headers())
                                .request(request)
                                .principal(NULL_PRINCIPAL);
                          }
                        });
              } else {
                // remote service
                Objects.requireNonNull(transport, "[requestOne] transport");
                return Mono.fromCallable(() -> serviceLookup(request))
                    .flatMap(
                        serviceReference ->
                            transport
                                .create(serviceReference)
                                .requestResponse(request, responseType)
                                .map(this::throwIfError));
              }
            })
        .doOnSuccess(
            response -> {
              if (logger != null && logger.isDebugEnabled()) {
                logger.debug(
                    "[{}] request: {}, response: {}", request.qualifier(), request, response);
              }
            })
        .doOnError(
            ex -> {
              if (logger != null) {
                logger.error("[{}][error] request: {}", request.qualifier(), request, ex);
              }
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
   * @param responseType type of responses (optional).
   * @return flux publisher of service responses.
   */
  public Flux<ServiceMessage> requestMany(ServiceMessage request, Type responseType) {
    return Flux.defer(
            () -> {
              ServiceMethodInvoker methodInvoker;
              if (serviceRegistry != null
                  && (methodInvoker = serviceRegistry.lookupInvoker(request)) != null) {
                // local service
                return methodInvoker
                    .invokeMany(request)
                    .map(this::throwIfError)
                    .contextWrite(
                        context -> {
                          if (context.hasKey(RequestContext.class)) {
                            return context;
                          } else {
                            return new RequestContext(context)
                                .headers(request.headers())
                                .request(request)
                                .principal(NULL_PRINCIPAL);
                          }
                        });
              } else {
                // remote service
                Objects.requireNonNull(transport, "[requestMany] transport");
                return Mono.fromCallable(() -> serviceLookup(request))
                    .flatMapMany(
                        serviceReference ->
                            transport
                                .create(serviceReference)
                                .requestStream(request, responseType)
                                .map(this::throwIfError));
              }
            })
        .doOnSubscribe(
            s -> {
              if (logger != null && logger.isDebugEnabled()) {
                logger.debug("[{}][subscribe] request: {}", request.qualifier(), request);
              }
            })
        .doOnComplete(
            () -> {
              if (logger != null && logger.isDebugEnabled()) {
                logger.debug("[{}][complete] request: {}", request.qualifier(), request);
              }
            })
        .doOnError(
            ex -> {
              if (logger != null) {
                logger.error("[{}][error] request: {}", request.qualifier(), request, ex);
              }
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
   * @param responseType type of responses (optional).
   * @return flux publisher of service responses.
   */
  public Flux<ServiceMessage> requestBidirectional(
      Publisher<ServiceMessage> publisher, Type responseType) {
    return Flux.from(publisher)
        .switchOnFirst(
            (first, messages) -> {
              if (first.hasValue()) {
                ServiceMessage request = first.get();
                ServiceMethodInvoker methodInvoker;
                if (serviceRegistry != null
                    && (methodInvoker = serviceRegistry.lookupInvoker(request)) != null) {
                  // local service
                  return methodInvoker.invokeBidirectional(messages).map(this::throwIfError);
                } else {
                  // remote service
                  Objects.requireNonNull(transport, "[requestBidirectional] transport");
                  return Mono.fromCallable(() -> serviceLookup(request))
                      .flatMapMany(
                          serviceReference ->
                              transport
                                  .create(serviceReference)
                                  .requestChannel(messages, responseType)
                                  .map(this::throwIfError));
                }
              }
              return messages;
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
    return (T)
        Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class[] {serviceInterface},
            (proxy, method, params) -> {
              Optional<Object> check =
                  toStringOrEqualsOrHashCode(method.getName(), serviceInterface, params);
              if (check.isPresent()) {
                return check.get(); // toString, hashCode was invoked.
              }

              final var serviceCall = this;
              final var methodInfo = Reflect.methodInfo(serviceInterface, method);
              final var returnType = methodInfo.parameterizedReturnType();
              final var isReturnTypeServiceMessage = methodInfo.isReturnTypeServiceMessage();
              final var request = methodInfo.requestType() == Void.TYPE ? null : params[0];

              //noinspection EnhancedSwitchMigration
              switch (methodInfo.communicationMode()) {
                case REQUEST_RESPONSE:
                  return serviceCall
                      .requestOne(toServiceMessage(methodInfo, request), returnType)
                      .transform(asMono(isReturnTypeServiceMessage));

                case REQUEST_STREAM:
                  return serviceCall
                      .requestMany(toServiceMessage(methodInfo, request), returnType)
                      .transform(asFlux(isReturnTypeServiceMessage));

                case REQUEST_CHANNEL:
                  // this is REQUEST_CHANNEL so it means params[0] must
                  // be a publisher - its safe to cast.
                  //noinspection rawtypes
                  return serviceCall
                      .requestBidirectional(
                          Flux.from((Publisher) request)
                              .map(data -> toServiceMessage(methodInfo, data)),
                          returnType)
                      .transform(asFlux(isReturnTypeServiceMessage));

                default:
                  throw new IllegalArgumentException(
                      "Communication mode is not supported: " + method);
              }
            });
  }

  private ServiceReference serviceLookup(ServiceMessage request) {
    return router
        .route(serviceRegistry, request)
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

  private static ServiceUnavailableException noReachableMemberException(ServiceMessage request) {
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

  private static Function<Flux<ServiceMessage>, Flux<Object>> asFlux(
      boolean isReturnTypeServiceMessage) {
    return flux ->
        isReturnTypeServiceMessage ? flux.cast(Object.class) : flux.map(ServiceMessage::data);
  }

  private static Function<Mono<ServiceMessage>, Mono<Object>> asMono(
      boolean isReturnTypeServiceMessage) {
    return mono ->
        isReturnTypeServiceMessage ? mono.cast(Object.class) : mono.map(ServiceMessage::data);
  }

  private ServiceMessage throwIfError(ServiceMessage message) {
    if (message.isError() && message.hasData(ErrorData.class)) {
      throw Exceptions.propagate(errorMapper.toError(message));
    }
    return message;
  }

  @Override
  public void close() {
    if (transport != null) {
      try {
        transport.close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
