package io.scalecube.services;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.metrics.Metrics;
import io.scalecube.services.routing.Router;
import io.scalecube.services.transport.client.api.ClientTransport;
import io.scalecube.transport.Address;

import com.codahale.metrics.Timer;
import com.google.common.reflect.Reflection;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ServiceCall {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceCall.class);
  private final ClientTransport transport;

  public ServiceCall(ClientTransport transport) {
    this.transport = transport;
  }

  public Call call() {
    return new Call(this.transport);
  }

  public static class Call {

    private Router router;
    private Metrics metrics;
    private Timer latency;
    private ClientTransport transport;
    private ServiceMessageCodec codec;

    public Call(ClientTransport transport) {
      this.transport = transport;
      this.codec = transport.getMessageCodec();
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
    public Publisher<ServiceMessage> requestResponse(final ServiceMessage request) {
      Class responseType = request.responseType() !=null ? request.responseType() : Object.class;
      return requestResponse(request, responseType);
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
    public Publisher<ServiceMessage> requestResponse(final ServiceMessage request, final Class<?> returnType) {
      Messages.validate().serviceRequest(request);
      return requestResponse(request, returnType,
          router.route(request).orElseThrow(() -> noReachableMemberException(request)));
    }


    /**
     * Invoke a request message and invoke a service by a given service name and method name. expected headers in
     * request:ServiceHeaders.SERVICE_REQUEST the logical name of the service. ServiceHeaders.METHOD the method name to
     *
     * @param request request with given headers.
     * @param serviceReference target instance to invoke.
     * @return Mono with service call dispatching result.
     * @throws Exception in case of an error or TimeoutException if no response if a given duration.
     */
    public Publisher<ServiceMessage> requestResponse(final ServiceMessage request, final Class<?> returnType,
        final ServiceReference serviceReference) {
      // FIXME: in request response its not good idea to create transport for address per call.
      // better to reuse same channel.
      Address address = Address.create(serviceReference.host(), serviceReference.port());
      return transport.create(address).requestResponse(request)
          .map(message -> codec.decodeData(message, returnType));
    }

    public Mono<Void> fireAndForget(ServiceMessage request){
      ServiceReference serviceReference = router.route(request).orElseThrow(() -> noReachableMemberException(request));
      return fireAndForget(request, serviceReference);
    }
    public Mono<Void> fireAndForget(ServiceMessage request,final ServiceReference serviceReference) {
      Address address = Address.create(serviceReference.host(), serviceReference.port());
      return transport.create(address).fireAndForget(request);
    }
    

    /**
     * sending subscription request message to a service that returns Observable.
     *
     * @param request containing subscription data.
     * @return rx.Observable for the specific stream.
     */
    public Publisher<ServiceMessage> listen(ServiceMessage request) {
      Messages.validate().serviceRequest(request);
      Class responseType = request.responseType() !=null ? request.responseType() : Object.class;
      ServiceReference serviceReference = router.route(request).orElseThrow(() -> noReachableMemberException(request));
      return this.listen(request, serviceReference,responseType);
    }

    public Publisher<ServiceMessage> listen(ServiceMessage request, ServiceReference serviceReference, Class responseType) {
      Address address = Address.create(serviceReference.host(), serviceReference.port());
      return transport.create(address).requestStream(request)
          .map(message -> codec.decodeData(message, responseType));
    }

    /**
     * Create proxy creates a java generic proxy instance by a given service interface.
     *
     * @param serviceInterface Service Interface type.
     * @return newly created service proxy object.
     */
    public <T> T api(final Class<T> serviceInterface) {

      final Call serviceCall = this;

      return Reflection.newProxy(serviceInterface, new InvocationHandler() {
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {

          Object check = objectToStringEqualsHashCode(method.getName(), serviceInterface, args);
          if (check != null) {
            return check; // toString, hashCode was invoked.
          }

          Class<?> parameterizedReturnType = Reflect.parameterizedReturnType(method);
          Class<?> returnType = method.getReturnType();

          Metrics.mark(serviceInterface, metrics, method, "request");
          Object data = method.getParameterCount() != 0 ? args[0] : null;
          final ServiceMessage reqMsg = ServiceMessage.builder()
              .qualifier(Reflect.serviceName(serviceInterface), method.getName())
              .data(data)
              .build();

          if (returnType.isAssignableFrom(Mono.class)) {
            return Mono.from(serviceCall.requestResponse(reqMsg, parameterizedReturnType))
                .map(message -> codec.decodeData(message, parameterizedReturnType))
                .transform(mono -> parameterizedReturnType.equals(ServiceMessage.class) ? mono
                    : mono.map(ServiceMessage::data));

          } else if (returnType.equals(Flux.class)) {
            return Flux.from(serviceCall.listen(reqMsg))
                .map(message -> codec.decodeData(message, parameterizedReturnType))
                .transform(flux -> parameterizedReturnType.equals(ServiceMessage.class) ? flux
                    : flux.map(ServiceMessage::data));

          } else if (returnType.equals(Void.TYPE)) {
            serviceCall.requestResponse(reqMsg, parameterizedReturnType);
            return null;
          } else {
            LOGGER.error("return value is not supported type.");
            return null;
          }
        }
      });
    }

    private IllegalStateException noReachableMemberException(ServiceMessage request) {

      LOGGER.error(
          "Failed  to invoke service, No reachable member with such service definition [{}], args [{}]",
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
