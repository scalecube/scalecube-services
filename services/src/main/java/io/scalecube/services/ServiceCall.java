package io.scalecube.services;

import io.scalecube.services.api.NullData;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codec.ServiceMessageDataCodec;
import io.scalecube.services.exceptions.ExceptionProcessor;
import io.scalecube.services.exceptions.ServiceUnavailableException;
import io.scalecube.services.metrics.Metrics;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.routing.Router;
import io.scalecube.services.routing.Routers;
import io.scalecube.services.transport.HeadAndTail;
import io.scalecube.services.transport.LocalServiceHandlers;
import io.scalecube.services.transport.client.api.ClientTransport;
import io.scalecube.transport.Address;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;

public class ServiceCall {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceCall.class);

  private final ClientTransport transport;
  private final LocalServiceHandlers serviceHandlers;
  private final ServiceRegistry serviceRegistry;
  private final Router router;
  private final Metrics metrics;
  private final ServiceMessageDataCodec dataCodec = new ServiceMessageDataCodec();

  ServiceCall(Call call) {
    this.transport = call.transport;
    this.serviceHandlers = call.serviceHandlers;
    this.serviceRegistry = call.serviceRegistry;
    this.router = call.router;
    this.metrics = call.metrics;
  }

  public static class Call {

    private Router router;
    private Metrics metrics;

    private final ClientTransport transport;
    private final LocalServiceHandlers serviceHandlers;
    private final ServiceRegistry serviceRegistry;


    public Call(ClientTransport transport,
        LocalServiceHandlers serviceHandlers,
        ServiceRegistry serviceRegistry) {
      this.transport = transport;
      this.serviceRegistry = serviceRegistry;
      this.serviceHandlers = serviceHandlers;
    }

    public Call router(Class<? extends Router> routerType) {
      this.router = Routers.getRouter(routerType);
      return this;
    }

    public Call router(Router router) {
      this.router = router;
      return this;
    }

    public Call metrics(Metrics metrics) {
      this.metrics = metrics;
      return this;
    }

    public ServiceCall create() {
      return new ServiceCall(this);
    }
  }

  /**
   * Issues fire-and-rorget request.
   *
   * @param request request message to send.
   * @return mono publisher completing normally or with error.
   */
  public Mono<Void> oneWay(ServiceMessage request) {
    return requestOne(request, Void.class).then();
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
    String qualifier = request.qualifier();
    if (serviceHandlers.contains(qualifier)) { // local service.
      return serviceHandlers.get(qualifier)
          .requestResponse(request)
          .onErrorMap(ExceptionProcessor::mapException);
    } else { // remote service.
      return transport.create(addressLookup(request))
          .requestResponse(request)
          .map(message -> dataCodec.decode(message, responseType));
    }
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
    String qualifier = request.qualifier();
    if (serviceHandlers.contains(qualifier)) { // local service.
      return serviceHandlers.get(qualifier)
          .requestStream(request)
          .onErrorMap(ExceptionProcessor::mapException);
    } else { // remote service.
      return transport.create(addressLookup(request))
          .requestStream(request)
          .map(message -> dataCodec.decode(message, responseType));
    }
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
  public Flux<ServiceMessage> requestBidirectional(Publisher<ServiceMessage> publisher, Class<?> responseType) {
    return Flux.from(HeadAndTail.createFrom(publisher)).flatMap(pair -> {
      ServiceMessage request = pair.head();
      String qualifier = request.qualifier();

      Flux<ServiceMessage> requestPublisher = Flux.from(pair.tail()).startWith(request);

      if (serviceHandlers.contains(qualifier)) { // local service.
        return serviceHandlers.get(qualifier)
            .requestChannel(requestPublisher)
            .onErrorMap(ExceptionProcessor::mapException);
      } else { // remote service.
        return transport.create(addressLookup(request))
            .requestChannel(requestPublisher)
            .map(message -> dataCodec.decode(message, responseType));
      }
    });
  }

  /**
   * Create proxy creates a java generic proxy instance by a given service interface.
   *
   * @param serviceInterface Service Interface type.
   * @return newly created service proxy object.
   */
  public <T> T api(Class<T> serviceInterface) {

    final ServiceCall serviceCall = this;
    final Map<Method, MethodInfo> genericReturnTypes = Reflect.methodsInfo(serviceInterface);

    // noinspection unchecked
    return (T) Proxy.newProxyInstance(getClass().getClassLoader(), new Class[] {serviceInterface},
        (proxy, method, params) -> {
          MethodInfo methodInfo = genericReturnTypes.get(method);
          Object check = objectToStringEqualsHashCode(method.getName(), serviceInterface, params);
          if (check != null) {
            return check; // toString, hashCode was invoked.
          }

          ServiceMessage request = ServiceMessage.builder()
              .qualifier(methodInfo.serviceName(), method.getName())
              .data(method.getParameterCount() != 0 ? params[0] : NullData.NULL_DATA)
              .build();

          Metrics.mark(serviceInterface, metrics, method, "request");

          switch (methodInfo.communicationMode()) {
            case FIRE_AND_FORGET:
              return serviceCall.oneWay(request);
            case REQUEST_RESPONSE:
              return serviceCall.requestOne(request, methodInfo.parameterizedReturnType())
                  .transform(mono -> methodInfo.isRequestTypeServiceMessage() ? mono : mono.map(ServiceMessage::data));
            case REQUEST_STREAM:
              return serviceCall.requestMany(request, methodInfo.parameterizedReturnType())
                  .transform(flux -> methodInfo.isRequestTypeServiceMessage() ? flux : flux.map(ServiceMessage::data));
            case REQUEST_CHANNEL:
              // falls to default
            default:
              throw new IllegalArgumentException("Communication mode is not supported: " + method);
          }
        });
  }

  private Address addressLookup(ServiceMessage request) {
    ServiceReference serviceReference =
        router.route(serviceRegistry, request)
            .orElseThrow(() -> noReachableMemberException(request));

    return Address.create(serviceReference.host(), serviceReference.port());
  }

  private static ServiceUnavailableException noReachableMemberException(ServiceMessage request) {
    LOGGER.error("Failed  to invoke service, No reachable member with such service definition [{}], args [{}]",
        request.qualifier(), request);
    return new ServiceUnavailableException("No reachable member with such service: " + request.qualifier());
  }

  private static Object objectToStringEqualsHashCode(String method, Class<?> serviceInterface, Object... args) {
    if ("hashCode".equals(method)) {
      return serviceInterface.hashCode();
    } else if ("equals".equals(method)) {
      return serviceInterface.equals(args[0]);
    } else if ("toString".equals(method)) {
      return serviceInterface.toString();
    } else {
      return null;
    }
  }
}
