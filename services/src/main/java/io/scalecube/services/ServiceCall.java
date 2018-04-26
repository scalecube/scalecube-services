package io.scalecube.services;

import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codecs.api.ServiceMessageCodec;
import io.scalecube.services.exceptions.ExceptionProcessor;
import io.scalecube.services.metrics.Metrics;
import io.scalecube.services.routing.Router;
import io.scalecube.services.transport.LocalServiceDispatchers;
import io.scalecube.services.transport.api.ServiceMethodDispatcher;
import io.scalecube.services.transport.client.api.ClientTransport;
import io.scalecube.transport.Address;

import com.codahale.metrics.Timer;
import com.google.common.reflect.Reflection;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ServiceCall {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceCall.class);
  private final ClientTransport transport;
  private final LocalServiceDispatchers localServices;

  public ServiceCall(ClientTransport transport, LocalServiceDispatchers localServices) {
    this.transport = transport;
    this.localServices = localServices;
  }

  public Call call() {
    return new Call(this.transport, this.localServices);
  }

  public static class Call {

    private Router router;
    private Metrics metrics;
    private Timer latency;
    private ClientTransport transport;
    private ServiceMessageCodec codec;
    private LocalServiceDispatchers localServices;

    public Call(ClientTransport transport, LocalServiceDispatchers localServices) {
      this.transport = transport;
      this.codec = transport.getMessageCodec();
      this.localServices = localServices;
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
     * Invoke a request message and invoke a service by a given service name and method name. expected headers in *
     * request: ServiceHeaders.SERVICE_REQUEST the logical name of the service. ServiceHeaders.METHOD the method name to
     * invoke message uses the router to select the target endpoint service instance in the cluster. Throws Exception
     * in* case of an error or TimeoutException if no response if a given duration.
     *
     * @param request request with given headers.
     * @return CompletableFuture with service call dispatching result.
     */
    public Publisher<ServiceMessage> requestOne(final ServiceMessage request) {
      Class responseType = request.responseType() != null ? request.responseType() : Object.class;
      return requestOne(request, responseType);
    }

    @SuppressWarnings("unchecked")
    public Publisher<ServiceMessage> requestOne(final ServiceMessage request, final Class<?> returnType) {
      Messages.validate().serviceRequest(request);
      String qualifier = request.qualifier();

      if (localServices.contains(qualifier)) {
        ServiceMethodDispatcher dispatcher = localServices.getDispatcher(qualifier);
        return ((Mono<ServiceMessage>) Mono.from(dispatcher.invoke((request))))
            .onErrorMap(ExceptionProcessor::mapException);
      } else {
        ServiceReference serviceReference =
            router.route(request).orElseThrow(() -> noReachableMemberException(request));

        Address address =
            Address.create(serviceReference.host(), serviceReference.port());

        // FIXME: in request response its not good idea to create transport for address per call.
        // better to reuse same channel.
        return transport.create(address)
            .requestResponse(request)
            .map(message -> {
              if (ExceptionProcessor.isError(message)) {
                throw ExceptionProcessor.toException(codec.decodeData(message, ErrorData.class));
              } else {
                return codec.decodeData(message, returnType);
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
    @SuppressWarnings("unchecked")
    public Mono<Void> oneWay(ServiceMessage request) {
      Messages.validate().serviceRequest(request);
      String qualifier = request.qualifier();

      if (localServices.contains(qualifier)) {
        ServiceMethodDispatcher dispatcher = localServices.getDispatcher(qualifier);
        return ((Mono<Void>) Mono.from(dispatcher.invoke((request))))
            .onErrorMap(ExceptionProcessor::mapException);
      } else {
        ServiceReference serviceReference =
            router.route(request).orElseThrow(() -> noReachableMemberException(request));

        Address address =
            Address.create(serviceReference.host(), serviceReference.port());

        return transport.create(address).fireAndForget(request);
      }
    }

    /**
     * sending subscription request message to a service that returns Observable.
     *
     * @param request containing subscription data.
     * @return rx.Observable for the specific stream.
     */
    @SuppressWarnings("unchecked")
    public Publisher<ServiceMessage> requestMany(ServiceMessage request) {
      Messages.validate().serviceRequest(request);
      String qualifier = request.qualifier();

      if (localServices.contains(qualifier)) {
        ServiceMethodDispatcher dispatcher = localServices.getDispatcher(qualifier);
        return ((Flux<ServiceMessage>) Flux.from(dispatcher.invoke((request))))
            .onErrorMap(ExceptionProcessor::mapException);
      } else {
        Class responseType =
            request.responseType() != null ? request.responseType() : Object.class;

        ServiceReference serviceReference =
            router.route(request).orElseThrow(() -> noReachableMemberException(request));

        Address address =
            Address.create(serviceReference.host(), serviceReference.port());

        return transport.create(address)
            .requestStream(request)
            .map(message -> {
              if (ExceptionProcessor.isError(message)) {
                throw ExceptionProcessor.toException(codec.decodeData(message, ErrorData.class));
              } else {
                return codec.decodeData(message, responseType);
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
        final ServiceMessage reqMsg = ServiceMessage.builder()
            .qualifier(Reflect.serviceName(serviceInterface), method.getName())
            .data(method.getParameterCount() != 0 ? args[0] : null)
            .build();


        if (returnType.isAssignableFrom(Mono.class) && parameterizedReturnType.isAssignableFrom(Void.class)) {
          return serviceCall.oneWay(reqMsg);

        } else if (returnType.isAssignableFrom(Mono.class)) {
          // noinspection unchecked
          return Mono.from(serviceCall.requestOne(reqMsg, parameterizedReturnType))
              .map(message -> codec.decodeData(message, parameterizedReturnType))
              .transform(mono -> parameterizedReturnType.equals(ServiceMessage.class) ? mono
                  : mono.map(ServiceMessage::data));

        } else if (returnType.isAssignableFrom(Flux.class)) {
          // noinspection unchecked
          return Flux.from(serviceCall.requestMany(reqMsg))
              .map(message -> codec.decodeData(message, parameterizedReturnType))
              .transform(flux -> parameterizedReturnType.equals(ServiceMessage.class) ? flux
                  : flux.map(ServiceMessage::data));

        } else if (returnType.isAssignableFrom(Void.class) || returnType.equals(Void.TYPE)) {
          serviceCall.oneWay(reqMsg);
          return null;

        } else {
          LOGGER.error("return value is not supported type.");
          return null;
        }
      });
    }

    private IllegalStateException noReachableMemberException(ServiceMessage request) {
      LOGGER.error("Failed  to invoke service, No reachable member with such service definition [{}], args [{}]",
          request.qualifier(), request);
      return new IllegalStateException("No reachable member with such service: " + request.qualifier());
    }

    private Object objectToStringEqualsHashCode(String method, Class<?> serviceInterface, Object... args) {
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
