package io.scalecube.services;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.cluster.membership.IdGenerator;
import io.scalecube.services.metrics.Metrics;
import io.scalecube.services.routing.Router;
import io.scalecube.transport.Message;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.time.Duration;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class ServiceCall {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceProxyFactory.class);

  private Duration timeout;
  private Router router;
  private Timer latency;
  private Metrics metrics;

  /**
   * ServiceCall is a service communication pattern for async request reply and reactive streams. it communicates with
   * local and remote services using messages and handles. it acts as proxy and middle-ware between service consumer and
   * service provider.
   * 
   * @param router strategy to select service instance.
   * @param timeout waiting for response.
   * @param metrics provider to collect metrics regards service execution.
   */
  public ServiceCall(Router router, Duration timeout, Metrics metrics) {
    this.router = router;
    this.timeout = timeout;
    this.metrics = metrics;
    this.latency = Metrics.timer(this.metrics, ServiceCall.class.getName(), "invoke");
  }

  /**
   * ServiceCall is a service communication pattern for async request reply and reactive streams. it communicates with
   * local and remote services using messages and handles. it acts as proxy and middle-ware between service consumer and
   * service provider.
   *
   * @param router strategy to select service instance.
   * @param timeout waiting for response.
   */
  public ServiceCall(Router router, Duration timeout) {
    this(router, timeout, null);
  }

  /**
   * Invokes and returns promise for invocation.
   * 
   * @param message message to sedn
   * @return promise
   */
  public CompletableFuture<Message> invoke(Message message) {
    return invoke(message, timeout);
  }

  /**
   * Invoke a request message and invoke a service by a given service name and method name. expected headers in request:
   * ServiceHeaders.SERVICE_REQUEST the logical name of the service. ServiceHeaders.METHOD the method name to invoke
   * message uses the router to select the target endpoint service instance in the cluster. Throws Exception in case of
   * an error or TimeoutException if no response if a given duration.
   * 
   * @param request request with given headers.
   * @param timeout timeout
   * @return CompletableFuture with service call dispatching result.
   */
  public CompletableFuture<Message> invoke(Message request, Duration timeout) {
    Messages.validate().serviceRequest(request);

    Optional<ServiceInstance> optionalServiceInstance = router.route(request);

    if (optionalServiceInstance.isPresent()) {
      ServiceInstance instance = optionalServiceInstance.get();
      return this.invoke(request, instance, timeout);
    } else {
      throw noReachableMemberException(request);
    }
  }

  /**
   * Invoke a request message and invoke a service by a given service name and method name. expected headers in request:
   * ServiceHeaders.SERVICE_REQUEST the logical name of the service. ServiceHeaders.METHOD the method name to invoke
   * with default timeout.
   * 
   * @param request request with given headers.
   * @param serviceInstance target instance to invoke.
   * @return CompletableFuture with service call dispatching result.
   * @throws Exception in case of an error or TimeoutException if no response if a given duration.
   */
  public CompletableFuture<Message> invoke(Message request, ServiceInstance serviceInstance) throws Exception {
    Messages.validate().serviceRequest(request);
    return invoke(request, serviceInstance, timeout);
  }

  /**
   * Invoke a request message and invoke a service by a given service name and method name. expected headers in request:
   * ServiceHeaders.SERVICE_REQUEST the logical name of the service. ServiceHeaders.METHOD the method name to invoke.
   * Throws Exception in case of an error or TimeoutException if no response if a given duration.
   * 
   * @param request request with given headers.
   * @param serviceInstance target instance to invoke.
   * @param duration of the response before TimeException is returned.
   * @return CompletableFuture with service call dispatching result.
   */
  public CompletableFuture<Message> invoke(final Message request, final ServiceInstance serviceInstance,
      final Duration duration) {

    Objects.requireNonNull(serviceInstance);
    Messages.validate().serviceRequest(request);
    serviceInstance.checkMethodExists(request.header(ServiceHeaders.METHOD));

    if (!serviceInstance.isLocal()) {
      String cid = IdGenerator.generateId();

      final ServiceResponse responseFuture = ServiceResponse.correlationId(cid);

      final Context ctx = Metrics.time(latency);
      Metrics.mark(this.metrics, ServiceCall.class.getName(), "invoke", "request");
      Counter counter = Metrics.counter(metrics, ServiceCall.class.getName(), "invoke-pending");
      Metrics.inc(counter);
      serviceInstance.invoke(Messages.asRequest(request, cid))
          .whenComplete((success, error) -> {
            Metrics.dec(counter);
            Metrics.stop(ctx);
            if (error == null) {
              Metrics.mark(metrics, ServiceCall.class, "invoke", "response");
              responseFuture.withTimeout(duration);
            } else {
              Metrics.mark(metrics, ServiceCall.class.getName(), "invoke", "error");
              responseFuture.completeExceptionally(error);
            }
          });

      return responseFuture.future();
    } else {
      return serviceInstance.invoke(request);
    }
  }



  /**
   * Invoke all service instances with a given request message with a given service name and method name. expected
   * headers in request: ServiceHeaders.SERVICE_REQUEST the logical name of the service. ServiceHeaders.METHOD the
   * method name to invoke. retrieves routes from router by calling router.routes and send async to each endpoint once a
   * response is returned emit the response to the observable. uses a default duration timeout configured for this
   * proxy.
   * 
   * @param request request with given headers.
   * @return Observable with stream of results for each service call dispatching result.
   */
  public Observable<Message> invokeAll(final Message request) {
    return this.invokeAll(request, this.timeout);
  }

  /**
   * Invoke all service instances with a given request message with a given service name and method name. expected
   * headers in request: ServiceHeaders.SERVICE_REQUEST the logical name of the service. ServiceHeaders.METHOD the
   * method name to invoke. retrieves routes from router by calling router.routes and send async to each endpoint once a
   * response is returned emit the response to the observable.
   * 
   * @param request request with given headers.
   * @param duration of the response before TimeException is returned.
   * @return Observable with stream of results for each service call dispatching result.
   */
  public Observable<Message> invokeAll(final Message request, final Duration duration) {
    final Subject<Message, Message> responsesSubject = PublishSubject.<Message>create().toSerialized();
    Collection<ServiceInstance> instances = router.routes(request);

    instances.forEach(instance -> {
      invoke(request, duration).whenComplete((resp, error) -> {
        if (resp != null) {
          responsesSubject.onNext(resp);
        } else {
          responsesSubject.onNext(Messages.asError(error, request.correlationId(), instance.memberId()));
        }
      });
    });
    return responsesSubject.onBackpressureBuffer().asObservable();
  }

  /**
   * sending subscription request message to a service that returns Observable.
   * 
   * @param request containing subscription data.
   * @return rx.Observable for the specific stream.
   */
  public Observable<Message> listen(Message request) {

    Messages.validate().serviceRequest(request);

    Optional<ServiceInstance> optionalServiceInstance = router.route(request);

    if (optionalServiceInstance.isPresent()) {
      ServiceInstance instance = optionalServiceInstance.get();
      checkArgument(instance.methodExists(request.header(ServiceHeaders.METHOD)),
          "instance has no such requested method");

      return instance.listen(request);
    } else {
      throw noReachableMemberException(request);
    }
  }

  private IllegalStateException noReachableMemberException(Message request) {
    String serviceName = request.header(ServiceHeaders.SERVICE_REQUEST);
    String methodName = request.header(ServiceHeaders.METHOD);

    LOGGER.error(
        "Failed  to invoke service, No reachable member with such service definition [{}], args [{}]",
        serviceName, request);
    return new IllegalStateException("No reachable member with such service: " + methodName);
  }
}
