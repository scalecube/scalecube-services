package io.scalecube.services;

import io.scalecube.services.routing.Router;
import io.scalecube.transport.Message;
import io.scalecube.transport.Message.Builder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ServiceCall {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceProxyFactory.class);

  /**
   * used to complete the request future with timeout exception in case no response comes from service.
   */
  private static final ScheduledExecutorService delayer =
      ThreadFactory.singleScheduledExecutorService("sc-services-timeout");

  private Duration timeout;
  private Router router;

  public ServiceCall(Router router, Duration timeout) {
    this.router = router;
    this.timeout = timeout;
  }

  public <T> CompletableFuture<Message> invoke(Message message) {
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
  public <T> CompletableFuture<Message> invoke(Message request, Duration timeout) {
    String serviceName = request.header(ServiceHeaders.SERVICE_REQUEST);
    String methodName = request.header(ServiceHeaders.METHOD);
    try {

      Optional<ServiceInstance> optionalServiceInstance = router.route(request);

      if (optionalServiceInstance.isPresent()) {
        return this.invoke(request, optionalServiceInstance.get(), timeout);
      } else {
        LOGGER.error(
            "Failed  to invoke service, No reachable member with such service definition [{}], args [{}]",
            serviceName, request);
        throw new IllegalStateException("No reachable member with such service: " + methodName);
      }

    } catch (Throwable ex) {
      LOGGER.error(
          "Failed  to invoke service, No reachable member with such service method [{}], args [{}], error [{}]",
          methodName, request.data(), ex);
      throw new IllegalStateException("No reachable member with such service: " + methodName);
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
  public <T> CompletableFuture<Message> invoke(Message request, ServiceInstance serviceInstance) throws Exception {
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
  @SuppressWarnings("unchecked")
  public <T> CompletableFuture<Message> invoke(Message request, ServiceInstance serviceInstance, Duration duration)
      throws Exception {

    if (serviceInstance.isLocal()) {
      CompletableFuture<?> resultFuture = (CompletableFuture<?>) serviceInstance.invoke(request);
      return (CompletableFuture<Message>) timeoutAfter(resultFuture, timeout)
          .thenApply(result -> toMessage(request, (T) result));
    } else {
      RemoteServiceInstance remote = (RemoteServiceInstance) serviceInstance;
      CompletableFuture<?> resultFuture =
          (CompletableFuture<?>) remote.dispatch(request);

      return (CompletableFuture<Message>) timeoutAfter(resultFuture, timeout);
    }
  }

  /**
   * listen on a message stream from endpoint. Sends a request message and invoke a service by a given service name and
   * method name. expected headers in request: ServiceHeaders.SERVICE_REQUEST the logical name of the service.
   * ServiceHeaders.METHOD the method name to invoke.
   * 
   * @param request request with given headers.
   * @return Message Observable stream from service requests.
   */
  public Observable<Message> listen(Message request) {

    String serviceName = request.header(ServiceHeaders.SERVICE_REQUEST);
    String methodName = request.header(ServiceHeaders.METHOD);
    try {

      Optional<ServiceInstance> optionalServiceInstance = router.route(request);

      if (optionalServiceInstance.isPresent()) {
        return this.listen(request, optionalServiceInstance.get(), timeout);
      } else {
        LOGGER.error(
            "Failed  to invoke service, No reachable member with such service definition [{}], args [{}]",
            serviceName, request);
        throw new IllegalStateException("No reachable member with such service: " + methodName);
      }

    } catch (Throwable ex) {
      LOGGER.error(
          "Failed  to invoke service, No reachable member with such service method [{}], args [{}], error [{}]",
          methodName, request.data(), ex);
      throw new IllegalStateException("No reachable member with such service: " + methodName);
    }

  }

  private <T> Observable<T> listen(Message request, ServiceInstance serviceInstance, Duration timeout)
      throws Exception {
    return (Observable<T>) serviceInstance.listen(request);
  }

  private <T> Message toMessage(Message request, T result) {
    if (result instanceof Message) {
      return (Message) result;
    } else {
      return Message.builder()
          .header(ServiceHeaders.SERVICE_RESPONSE, request.header(ServiceHeaders.SERVICE_REQUEST))
          .header(ServiceHeaders.METHOD, request.header(ServiceHeaders.METHOD))
          .correlationId(request.correlationId())
          .qualifier(request.qualifier())
          .data(result)
          .build();
    }
  }

  private CompletableFuture<?> timeoutAfter(final CompletableFuture<?> resultFuture, Duration timeout) {

    final CompletableFuture<Class<Void>> timeoutFuture = new CompletableFuture<>();

    // schedule to terminate the target goal in future in case it was not done yet
    final ScheduledFuture<?> scheduledEvent = delayer.schedule(() -> {
      // by this time the target goal should have finished.
      if (!resultFuture.isDone()) {
        // target goal not finished in time so cancel it with timeout.
        resultFuture.completeExceptionally(new TimeoutException("expecting response reached timeout!"));
      }
    }, timeout.toMillis(), TimeUnit.MILLISECONDS);

    // cancel the timeout in case target goal did finish on time
    if (resultFuture != null) {
      resultFuture.thenRun(() -> {
        if (resultFuture.isDone()) {
          if (!scheduledEvent.isDone()) {
            scheduledEvent.cancel(false);
          }
          timeoutFuture.complete(Void.TYPE);
        }
      });
    } else {
      return CompletableFuture.completedFuture(null);
    }
    return resultFuture;
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
        .header(ServiceHeaders.METHOD, methodName);

  }


}
