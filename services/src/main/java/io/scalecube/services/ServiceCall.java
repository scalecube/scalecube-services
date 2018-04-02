package io.scalecube.services;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.concurrency.Futures;
import io.scalecube.services.metrics.Metrics;
import io.scalecube.services.routing.Router;
import io.scalecube.streams.StreamMessage;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.common.reflect.Reflection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

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
     * Invoke a request message and invoke a service by a given service name and method name. expected headers in
     * request: ServiceHeaders.SERVICE_REQUEST the logical name of the service. ServiceHeaders.METHOD the method name to
     * invoke message uses the router to select the target endpoint service instance in the cluster. Throws Exception in
     * case of an error or TimeoutException if no response if a given duration.
     * 
     * @param request request with given headers.
     * @param timeout timeout
     * @return CompletableFuture with service call dispatching result.
     */
    public CompletableFuture<StreamMessage> invoke(StreamMessage request) {
      Messages.validate().serviceRequest(request);

      return this.invoke(request, router.route(request)
          .orElseThrow(() -> noReachableMemberException(request)), timeout);
    }

    /**
     * Invoke a request message and invoke a service by a given service name and method name. expected headers in
     * request: ServiceHeaders.SERVICE_REQUEST the logical name of the service. ServiceHeaders.METHOD the method name to
     * invoke with default timeout.
     * 
     * @param request request with given headers.
     * @param serviceInstance target instance to invoke.
     * @return CompletableFuture with service call dispatching result.
     * @throws Exception in case of an error or TimeoutException if no response if a given duration.
     */
    public CompletableFuture<StreamMessage> invoke(StreamMessage request, ServiceInstance serviceInstance)
        throws Exception {
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
    public CompletableFuture<StreamMessage> invoke(final StreamMessage request, final ServiceInstance serviceInstance,
        final Duration duration) {

      Objects.requireNonNull(serviceInstance);
      Messages.validate().serviceRequest(request);
      serviceInstance.checkMethodExists(Messages.qualifierOf(request).getAction());

      final Context ctx = Metrics.time(latency);
      Metrics.mark(this.metrics, ServiceCall.class.getName(), "invoke", "request");
      Counter counter = Metrics.counter(metrics, ServiceCall.class.getName(), "invoke-pending");
      Metrics.inc(counter);

      CompletableFuture<StreamMessage> response = serviceInstance.invoke(request);
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
     * Invoke all service instances with a given request message with a given service name and method name. expected
     * headers in request: ServiceHeaders.SERVICE_REQUEST the logical name of the service. ServiceHeaders.METHOD the
     * method name to invoke. retrieves routes from router by calling router.routes and send async to each endpoint once
     * a response is returned emit the response to the observable. uses a default duration timeout configured for this
     * proxy.
     * 
     * @param request request with given headers.
     * @return Observable with stream of results for each service call dispatching result.
     */
    public Observable<StreamMessage> invokeAll(final StreamMessage request) {
      return this.invokeAll(request, this.timeout);
    }

    /**
     * Invoke all service instances with a given request message with a given service name and method name. expected
     * headers in request: ServiceHeaders.SERVICE_REQUEST the logical name of the service. ServiceHeaders.METHOD the
     * method name to invoke. retrieves routes from router by calling router.routes and send async to each endpoint once
     * a response is returned emit the response to the observable.
     * 
     * @param request request with given headers.
     * @param duration of the response before TimeException is returned.
     * @return Observable with stream of results for each service call dispatching result.
     */
    public Observable<StreamMessage> invokeAll(final StreamMessage request, final Duration duration) {
      final Subject<StreamMessage, StreamMessage> responsesSubject =
          PublishSubject.<StreamMessage>create().toSerialized();
      Collection<ServiceInstance> instances = router.routes(request);

      instances.forEach(instance -> {
        invoke(request).whenComplete((resp, error) -> {
          if (resp != null) {
            responsesSubject.onNext(resp);
          } else {
            responsesSubject.onNext(Messages.asError(new Error(instance.memberId(), error)));
          }
        });
      });
      return responsesSubject.onBackpressureBuffer();
    }

    /**
     * sending subscription request message to a service that returns Observable.
     * 
     * @param request containing subscription data.
     * @return rx.Observable for the specific stream.
     */
    public Observable<StreamMessage> listen(StreamMessage request) {

      Messages.validate().serviceRequest(request);

      ServiceInstance instance = router.route(request)
          .orElseThrow(() -> noReachableMemberException(request));
      checkArgument(instance.methodExists(Messages.qualifierOf(request).getAction()),
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
      final Call service = this;

      return Reflection.newProxy(serviceInterface, new InvocationHandler() {
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
          Object check = objectToStringEqualsHashCode(method, serviceInterface, args);
          if (check != null)
            return check;
          
          Metrics.mark(serviceInterface, metrics, method, "request");
          Object data = method.getParameterCount() != 0 ? args[0] : null;
          final StreamMessage reqMsg = StreamMessage.builder()
              .qualifier(Reflect.serviceName(serviceInterface), method.getName())
              .data(data)
              .build();

          if (method.getReturnType().equals(Observable.class)) {
            if (Reflect.parameterizedReturnType(method).equals(StreamMessage.class)) {
              return service.listen(reqMsg);
            } else {
              return service.listen(reqMsg).map(message -> message.data());
            }
          } else {
            return toReturnValue(method, service.invoke(reqMsg));
          }
        }

        @SuppressWarnings("unchecked")
        private CompletableFuture<T> toReturnValue(final Method method, final CompletableFuture<StreamMessage> reuslt) {
          final CompletableFuture<T> future = new CompletableFuture<>();

          if (method.getReturnType().equals(Void.TYPE)) {
            return (CompletableFuture<T>) CompletableFuture.completedFuture(Void.TYPE);

          } else if (method.getReturnType().equals(CompletableFuture.class)) {
            reuslt.whenComplete((value, ex) -> {
              if (ex == null) {
                Metrics.mark(serviceInterface, metrics, method, "response");
                if (!Reflect.parameterizedReturnType(method).equals(StreamMessage.class)) {
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
          } else {
            LOGGER.error("return value is not supported type.");
            future.completeExceptionally(new UnsupportedOperationException());
          }
          return future;
        }
      });
    }

    private IllegalStateException noReachableMemberException(StreamMessage request) {

      LOGGER.error(
          "Failed  to invoke service, No reachable member with such service definition [{}], args [{}]",
          request.qualifier(), request);
      return new IllegalStateException("No reachable member with such service: " + request.qualifier());
    }
    
    private Object objectToStringEqualsHashCode(Method method, Class<?> serviceInterface, Object... args) {
      if (method.getName().equals("hashCode")) {
        return serviceInterface.hashCode();
      } else if (method.getName().equals("equals")) {
        return serviceInterface.equals(args[0]);
      } else if (method.getName().equals("toString")) {
        return serviceInterface.toString();
      } else {
        return null;
      }
    }
  }
}
