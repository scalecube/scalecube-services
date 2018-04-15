package io.scalecube.services;

import static java.util.Objects.requireNonNull;

import io.scalecube.concurrency.Futures;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.metrics.Metrics;
import io.scalecube.services.routing.Router;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.common.reflect.Reflection;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import reactor.core.publisher.Flux;

public class ServiceCall {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceCall.class);

  public static Call call() {
    return new Call();
  }

  public static class Call {

    private Duration timeout = Duration.ofSeconds(30);
    private Router router;
    private Metrics metrics;
    private Timer latency;
    private Class<?> responseType;

    public Call timeout(Duration timeout) {
      this.timeout = timeout;
      return this;
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
     * invokemessage uses the router to select the target endpoint service instance in the cluster. Throws Exception in*
     * case of an error or TimeoutException if no response if a given duration.
     *
     * @param request request with given headers.
     * @return CompletableFuture with service call dispatching result.
     */
    public CompletableFuture<ServiceMessage> invoke(ServiceMessage request) {
      Messages.validate().serviceRequest(request);

      ServiceInstance serviceInstance = router.route(request).orElseThrow(() -> noReachableMemberException(request));
      return invoke(request, serviceInstance, timeout);
    }


    /**
     * Invoke a request message and invoke a service by a given service name and method name. expected headers in
     * request:ServiceHeaders.SERVICE_REQUEST the logical name of the service. ServiceHeaders.METHOD the method name to
     * * invoke with default timeout.
     *
     * @param request request with given headers.
     * @param serviceInstance target instance to invoke.
     * @return CompletableFuture with service call dispatching result.
     * @throws Exception in case of an error or TimeoutException if no response if a given duration.
     */
    public CompletableFuture<ServiceMessage> invoke(ServiceMessage request, ServiceInstance serviceInstance) {
      Messages.validate().serviceRequest(request);
      return invoke(request, serviceInstance, timeout);
    }

    /**
     * Invoke a request message and invoke a service by a given service name and method name. expected headers in
     * request: ServiceHeaders.SERVICE_REQUEST the logical name of the service. ServiceHeaders.METHOD the method name to
     * invoke. Throws Exception in case of an error or TimeoutException if no response if a given duration.
     *
     * @param request request with given headers.
     * @param serviceInstance target instance to invoke.
     * @param duration of the response before TimeException is returned.
     * @return CompletableFuture with service call dispatching result.
     */
    public CompletableFuture<ServiceMessage> invoke(final ServiceMessage request, final ServiceInstance serviceInstance,
        final Duration duration) {

      Objects.requireNonNull(serviceInstance);
      Messages.validate().serviceRequest(request);
      serviceInstance.checkMethodExists(Messages.qualifierOf(request).getAction());

      final Context ctx = Metrics.time(latency);
      Metrics.mark(this.metrics, ServiceCall.class.getName(), "invoke", "request");
      Counter counter = Metrics.counter(metrics, ServiceCall.class.getName(), "invoke-pending");
      Metrics.inc(counter);

      CompletableFuture<ServiceMessage> response = serviceInstance.invoke(request);
      Futures.withTimeout(response, duration)
          .whenComplete((value, error) -> {
            Metrics.dec(counter);
            Metrics.stop(ctx);
            if (error == null) {
              Metrics.mark(metrics, ServiceCall.class, "invoke", "response");
              response.complete(value);
            } else {
              Metrics.mark(metrics, ServiceCall.class.getName(), "invoke", "error");
              response.completeExceptionally(error);
            }
          });
      return response;

    }

    /**
     * sending subscription request message to a service that returns Observable.
     *
     * @param request containing subscription data.
     * @return rx.Observable for the specific stream.
     */
    public Flux<ServiceMessage> listen(ServiceMessage request) {
      Objects.requireNonNull(responseType, "response type is not set");
      Messages.validate().serviceRequest(request);

      ServiceInstance instance = router.route(request)
          .orElseThrow(() -> noReachableMemberException(request));
      requireNonNull(instance.methodExists(Messages.qualifierOf(request).getAction()),
          "instance has no such requested method");

      return instance.listen(request);

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

          } else if (method.getReturnType().equals(CompletableFuture.class)) {
            return toCompletableFuture(method, serviceCall.invoke(reqMsg));

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
  }
}
