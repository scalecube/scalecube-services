package io.scalecube.services;

import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codec.ServiceMessageDataCodec;
import io.scalecube.services.exceptions.ExceptionProcessor;
import io.scalecube.services.exceptions.ServiceUnavailableException;
import io.scalecube.services.metrics.Metrics;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.routing.Router;
import io.scalecube.services.transport.LocalServiceHandlers;
import io.scalecube.services.transport.client.api.ClientTransport;
import io.scalecube.transport.Address;

import com.codahale.metrics.Timer;
import com.google.common.reflect.Reflection;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static io.scalecube.services.CommunicationMode.FIRE_AND_FORGET;
import static io.scalecube.services.CommunicationMode.REQUEST_CHANNEL;
import static io.scalecube.services.CommunicationMode.REQUEST_RESPONSE;
import static io.scalecube.services.CommunicationMode.REQUEST_STREAM;

public class ServiceCall {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceCall.class);

  private final ClientTransport transport;
  private final LocalServiceHandlers serviceHandlers;
  private final ServiceRegistry serviceRegistry;

  public ServiceCall(ClientTransport transport,
      LocalServiceHandlers serviceHandlers,
      ServiceRegistry serviceRegistry) {
    this.transport = transport;
    this.serviceHandlers = serviceHandlers;
    this.serviceRegistry = serviceRegistry;
  }

  public Call call() {
    return new Call(this.transport, this.serviceHandlers, this.serviceRegistry);
  }

  public static class Call {

    private Router router;
    private Metrics metrics;
    private Timer latency;
    private ClientTransport transport;
    private ServiceMessageDataCodec dataCodec;
    private LocalServiceHandlers serviceHandlers;
    private final ServiceRegistry serviceRegistry;

    public Call(ClientTransport transport,
        LocalServiceHandlers serviceHandlers,
        ServiceRegistry serviceRegistry) {
      this.transport = transport;
      this.serviceRegistry = serviceRegistry;
      this.dataCodec = new ServiceMessageDataCodec();
      this.serviceHandlers = serviceHandlers;
    }

    public Call router(Router router) {
      this.router = router;
      return this;
    }

    public Call metrics(Metrics metrics) {
      this.metrics = metrics;
      this.latency = Metrics.timer(this.metrics, ServiceCall.class.getName(), "invoke");
      return this;
    }

    /**
     * Invoke a request message and invoke a service by a given service name and method name. expected headers in
     * request: ServiceHeaders.SERVICE_REQUEST the logical name of the service. ServiceHeaders.METHOD the method name to
     * invoke message uses the router to select the target endpoint service instance in the cluster. Throws Exception in
     * case of an error or TimeoutException if no response if a given duration.
     *
     * @param request request with given headers.
     * @return {@link Publisher} with service call dispatching result.
     */
    public Publisher<ServiceMessage> requestOne(final ServiceMessage request) {
      return requestOne(request, request.responseType() != null ? request.responseType() : Object.class);
    }

    public Publisher<ServiceMessage> requestOne(final ServiceMessage request, final Class<?> returnType) {
      Messages.validate().serviceRequest(request);
      String qualifier = request.qualifier();

      if (serviceHandlers.contains(qualifier)) {
        return Mono
            .from(serviceHandlers.get(qualifier).invoke(Mono.just(request)))
            .onErrorMap(ExceptionProcessor::mapException);
      } else {
        ServiceReference serviceReference =
            router.route(serviceRegistry, request).orElseThrow(() -> noReachableMemberException(request));

        Address address =
            Address.create(serviceReference.host(), serviceReference.port());

        return transport.create(address)
            .requestResponse(request)
            .map(message -> {
              if (ExceptionProcessor.isError(message)) {
                throw ExceptionProcessor.toException(dataCodec.decode(message, ErrorData.class));
              } else {
                return dataCodec.decode(message, returnType);
              }
            });
      }
    }

    /**
     * Issues fire-and-rorget request.
     *
     * @param request request to send.
     * @return Mono of type Void.
     */
    public Mono<Void> oneWay(ServiceMessage request) {
      Messages.validate().serviceRequest(request);
      String qualifier = request.qualifier();

      if (serviceHandlers.contains(qualifier)) {
        return Mono
            .from(serviceHandlers.get(qualifier).invoke(Mono.just(request)))
            .onErrorMap(ExceptionProcessor::mapException)
            .map(message -> null);
      } else {
        ServiceReference serviceReference =
            router.route(serviceRegistry, request).orElseThrow(() -> noReachableMemberException(request));

        Address address =
            Address.create(serviceReference.host(), serviceReference.port());

        return transport.create(address).fireAndForget(request);
      }
    }

    /**
     * sending subscription request message to a service that returns Publisher.
     *
     * @param request containing subscription data.
     * @return Publisher for the specific stream.
     */
    public Publisher<ServiceMessage> requestMany(ServiceMessage request) {
      Messages.validate().serviceRequest(request);
      String qualifier = request.qualifier();

      if (serviceHandlers.contains(qualifier)) {
        return Flux
            .from(serviceHandlers.get(qualifier).invoke(Mono.just(request)))
            .onErrorMap(ExceptionProcessor::mapException);
      } else {
        ServiceReference serviceReference =
            router.route(serviceRegistry, request).orElseThrow(() -> noReachableMemberException(request));

        Address address =
            Address.create(serviceReference.host(), serviceReference.port());

        return transport.create(address)
            .requestStream(request)
            .map(message -> {
              if (ExceptionProcessor.isError(message)) {
                throw ExceptionProcessor.toException(dataCodec.decode(message, ErrorData.class));
              } else {
                Class returnType = request.responseType() != null ? request.responseType() : Object.class;
                return dataCodec.decode(message, returnType);
              }
            });
      }
    }

    /**
     * Create proxy creates a java generic proxy instance by a given service interface.
     *
     * @param serviceInterface Service Interface type.
     * @return newly created service proxy object.
     */
    public <T> T api(final Class<T> serviceInterface) {

      final Call serviceCall = this;

      return Reflection.newProxy(serviceInterface, (proxy, method, args) -> {

        Object check = objectToStringEqualsHashCode(method.getName(), serviceInterface, args);
        if (check != null) {
          return check; // toString, hashCode was invoked.
        }

        Metrics.mark(serviceInterface, metrics, method, "request");
        Class<?> parameterizedReturnType = Reflect.parameterizedReturnType(method);
        Class<?> returnType = method.getReturnType();
        Class<?> requestType = Reflect.requestType(method);
        CommunicationMode mode = Reflect.communicationMode(method);

        ServiceMessage request = ServiceMessage.builder()
            .qualifier(Reflect.serviceName(serviceInterface), method.getName())
            .data(method.getParameterCount() != 0 ? args[0] : null)
            .build();

        if (mode == FIRE_AND_FORGET) {
          // FireAndForget
          return serviceCall.oneWay(request);
        }
        if (mode == REQUEST_RESPONSE) {
          // RequestResponse
          return Mono
              .from(serviceCall.requestOne(request, parameterizedReturnType))
              .transform(mono -> parameterizedReturnType.equals(ServiceMessage.class) ? mono
                  : mono.map(ServiceMessage::data));
        }
        if (mode == REQUEST_CHANNEL) {
          // RequestChannel
          throw new IllegalArgumentException("REQUEST_CHANNEL mode is not supported: " + method);
        }
        if (mode == REQUEST_CHANNEL) {
          // RequestStream
          return Flux
              .from(serviceCall.requestMany(request))
              .transform(flux -> parameterizedReturnType.equals(ServiceMessage.class) ? flux
                  : flux.map(ServiceMessage::data));
        } else {
          throw new IllegalArgumentException(
              "Service method is not supported (check return type or parameter type): " + method);
        }
      });
    }

    private static ServiceUnavailableException noReachableMemberException(ServiceMessage request) {
      LOGGER.error("Failed  to invoke service, No reachable member with such service definition [{}], args [{}]",
          request.qualifier(), request);
      return new ServiceUnavailableException("No reachable member with such service: " + request.qualifier());
    }

    private static Object objectToStringEqualsHashCode(String method, Class<?> serviceInterface, Object... args) {
      if (method.equals("hashCode")) {
        return serviceInterface.hashCode();
      } else if (method.equals("equals")) {
        return serviceInterface.equals(args[0]);
      } else if (method.equals("toString")) {
        return serviceInterface.toString();
      } else {
        return null;
      }
    }
  }
}
