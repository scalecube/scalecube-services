package io.scalecube.services;

import io.scalecube.cluster.membership.IdGenerator;
import io.scalecube.services.routing.Router;
import io.scalecube.transport.Message;
import io.scalecube.transport.Message.Builder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;

import java.time.Duration;
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

    Optional<ServiceInstance> optionalServiceInstance = router.route(request);

    if (optionalServiceInstance.isPresent()) {
      return this.invoke(request, optionalServiceInstance.get(), timeout);
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

    if (!serviceInstance.isLocal()) {
      String cid = IdGenerator.generateId();

      Message requestMessage = asRequest(request, cid);

      final ServiceResponse responseFuture = ServiceResponse.correlationId(cid);

      serviceInstance.invoke(requestMessage).whenComplete((success, error) -> {
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
   * sending subscription request message to a service that returns Observable.
   * 
   * @param request containing subscription data.
   * @return rx.Observable for the specific stream.
   */
  public Observable<Message> listen(Message request) {
    Optional<ServiceInstance> optionalServiceInstance = router.route(request);

    if (optionalServiceInstance.isPresent()) {
      Observable<Message> subscription = optionalServiceInstance.get().listen(request);

      return subscription;
    } else {
      throw noReachableMemberException(request);
    }
  }


  /**
   * helper method to get service request builder with needed headers.
   * 
   * @param serviceName the requested service name.
   * @param methodName the requested service method name.
   * @return Builder for requested message.
   */
  public static Builder request(String serviceName, String methodName) {
    return Message.builder()
        .header(ServiceHeaders.SERVICE_REQUEST, serviceName)
        .header(ServiceHeaders.METHOD, methodName)
        .correlationId(IdGenerator.generateId());

  }

  private Message asRequest(Message request, final String correlationId) {
    return Message.withData(request.data())
        .header(ServiceHeaders.SERVICE_REQUEST, request.header(ServiceHeaders.SERVICE_REQUEST))
        .header(ServiceHeaders.METHOD, request.header(ServiceHeaders.METHOD))
        .correlationId(correlationId)
        .build();
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
