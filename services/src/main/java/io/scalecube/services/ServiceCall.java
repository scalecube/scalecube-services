package io.scalecube.services;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.metrics.Metrics;
import io.scalecube.services.routing.Router;
import io.scalecube.services.transport.client.api.ClientTransport;

import com.codahale.metrics.Timer;
import com.google.common.reflect.Reflection;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;

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

    public Call(ClientTransport transport) {
      this.transport = transport;
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
    public Mono<ServiceMessage> requestResponse(ServiceMessage request) {
      Messages.validate().serviceRequest(request);
      return requestResponse(request, router.route(request).orElseThrow(() -> noReachableMemberException(request)));
    }


    /**
     * Invoke a request message and invoke a service by a given service name and method name. expected headers in
     * request:ServiceHeaders.SERVICE_REQUEST the logical name of the service. ServiceHeaders.METHOD the method name to
     *
     * @param request request with given headers.
     * @param serviceInstance target instance to invoke.
     * @return Mono with service call dispatching result.
     * @throws Exception in case of an error or TimeoutException if no response if a given duration.
     */
    public Mono<ServiceMessage> requestResponse(ServiceMessage request, ServiceInstance serviceInstance) {
      return transport.create(serviceInstance.address())
          .requestResponse(request);
    }


    /**
     * sending subscription request message to a service that returns Observable.
     *
     * @param request containing subscription data.
     * @return rx.Observable for the specific stream.
     */
    public Flux<ServiceMessage> listen(ServiceMessage request) {
      Messages.validate().serviceRequest(request);
      ServiceInstance service = router.route(request)
          .orElseThrow(() -> noReachableMemberException(request));
      return this.listen(request, service);
    }

    public Flux<ServiceMessage> listen(ServiceMessage request, ServiceInstance instance) {
      return transport.create(instance.address)
          .requestStream(request);
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

          Metrics.mark(serviceInterface, metrics, method, "request");
          Object data = method.getParameterCount() != 0 ? args[0] : null;
          final ServiceMessage reqMsg = ServiceMessage.builder()
              .qualifier(Reflect.serviceName(serviceInterface), method.getName())
              .data(data)
              .build();

          if (method.getReturnType().getClass().isAssignableFrom(Publisher.class)) {
            if (Reflect.parameterizedReturnType(method).equals(ServiceMessage.class)) {
              return serviceCall.listen(reqMsg);
            } else {
              return serviceCall.listen(reqMsg).map(ServiceMessage::data);
            }

          } else if (method.getReturnType().equals(Flux.class)) {
            return serviceCall.requestResponse(reqMsg);

          } else if (method.getReturnType().equals(Void.TYPE)) {
            return CompletableFuture.completedFuture(Void.TYPE);

          } else {
            LOGGER.error("return value is not supported type.");
            return new CompletableFuture<T>().completeExceptionally(new UnsupportedOperationException());
          }
        }

        @SuppressWarnings("unchecked")
        private CompletableFuture<T> toCompletableFuture(final Method method,
            final CompletableFuture<ServiceMessage> reuslt) {
          final CompletableFuture<T> future = new CompletableFuture<>();
          reuslt.whenComplete((value, ex) -> {
            if (ex == null) {
              Metrics.mark(serviceInterface, metrics, method, "response");
              if (!Reflect.parameterizedReturnType(method).equals(ServiceMessage.class)) {
                future.complete((T) value.data());
              } else {
                future.complete((T) value);
              }
            } else {
              Metrics.mark(serviceInterface, metrics, method, "error");
              LOGGER.error("return value is exception: {}", ex);
              future.completeExceptionally(ex);
            }
          });
          return future;
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

    public Flux<ServiceMessage> channel(Publisher<ServiceMessage> request) {
      // TODO Auto-generated method stub
      return null;
    }

    public Mono<Void> fireAndForget(ServiceMessage request) {
      // TODO Auto-generated method stub
      return null;
    }
  }
}
