package io.scalecube.services;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.cluster.membership.IdGenerator;
import io.scalecube.services.routing.Router;
import io.scalecube.transport.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class ServiceCall {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceProxyFactory.class);

  private Duration timeout;
  private Router router;

  public ServiceCall(Router router, Duration timeout) {
    this.router = router;
    this.timeout = timeout;
  }

  public CompletableFuture<Message> invoke(Message message) {
    return invoke(message, timeout);
  }

  /**
   * Dispatch a request message and invoke a service by a given service name and method name. expected headers in
   * request: ServiceHeaders.SERVICE_REQUEST the logical name of the service. ServiceHeaders.METHOD the method name to
   * invoke message uses the router to select the target endpoint service instance in the cluster.
   * 
   * @param request request with given headers.
   * @timeout duration of the response before TimeException is returned.
   * @return CompletableFuture with service call dispatching result.
   * @throws Exception in case of an error or TimeoutException if no response if a given duration.
   */
  public CompletableFuture<Message> invoke(Message request, Duration timeout) {
    Messages.validate().serviceRequest(request);

    Optional<ServiceInstance> optionalServiceInstance = router.route(request);

    if (optionalServiceInstance.isPresent()) {
      ServiceInstance instance = optionalServiceInstance.get();
      validateHasMethod(request, instance);
      return this.invoke(request, instance, timeout);
    } else {
      throw noReachableMemberException(request);
    }
  }

  /**
   * Dispatch a request message and invoke a service by a given service name and method name. expected headers in
   * request: ServiceHeaders.SERVICE_REQUEST the logical name of the service. ServiceHeaders.METHOD the method name to
   * invoke with default timeout.
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
   * Dispatch a request message and invoke a service by a given service name and method name. expected headers in
   * request: ServiceHeaders.SERVICE_REQUEST the logical name of the service. ServiceHeaders.METHOD the method name to
   * invoke.
   * 
   * @param request request with given headers.
   * @param serviceInstance target instance to invoke.
   * @param duration of the response before TimeException is returned.
   * @return CompletableFuture with service call dispatching result.
   * @throws Exception in case of an error or TimeoutException if no response if a given duration.
   */
  public CompletableFuture<Message> invoke(final Message request, final ServiceInstance serviceInstance,
      final Duration duration) {

    Messages.validate().serviceRequest(request);
    validateHasMethod(request, serviceInstance);

    if (!serviceInstance.isLocal()) {
      String cid = IdGenerator.generateId();

      final ServiceResponse responseFuture = ServiceResponse.correlationId(cid);

      serviceInstance.invoke(Messages.asRequest(request, cid))
          .whenComplete((success, error) -> {
            if (error == null) {
              responseFuture.withTimeout(duration);
            } else {
              responseFuture.completeExceptionally(error);
            }
          });

      return responseFuture.future();
    } else {
      return serviceInstance.invoke(request);
    }
  }

  /**
   * Dispatch a request message and invoke all service endpoints by a given service name and method name. expected
   * headers in request: ServiceHeaders.SERVICE_REQUEST the logical name of the service. ServiceHeaders.METHOD the
   * method name to invoke. retrieves routes from router by calling router.routes and send async to each endpoint once a
   * response is returned emit the response to the observable.
   * 
   * @param request request with given headers.
   * @param duration of the response before TimeException is returned.
   * @return Observable with stream of results for each service call dispatching result.
   */
  public Observable<Message> invokeAll(final Message request, final Duration duration) {
    Subject<Message, Message> responsesSubject = PublishSubject.<Message>create().toSerialized();
    Collection<ServiceInstance> instances = router.routes(request);
    instances.forEach(instance -> {
      invoke(request, duration).whenComplete((resp, error) -> {
        if (resp != null) {
          responsesSubject.onNext(resp);
        } else {
          responsesSubject.onNext(Messages.asError(error,request.correlationId(),instance.memberId()));
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
      validateHasMethod(request, instance);

      return instance.listen(request);
    } else {
      throw noReachableMemberException(request);
    }
  }

  private void validateHasMethod(Message request, ServiceInstance instance) {
    checkArgument(instance.hasMethod(request.header(ServiceHeaders.METHOD)),
        "instance has no such requested method");
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
