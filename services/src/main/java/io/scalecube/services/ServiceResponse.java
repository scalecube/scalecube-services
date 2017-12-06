package io.scalecube.services;

import io.scalecube.transport.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * ServiceResponse handles the response of a service request based on correlationId. it holds a mapping between
 * correlationId to CompleteableFuture so when a response message is returned from service endpoint the future is
 * completed with success or error.
 */
public class ServiceResponse {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceResponse.class);

  private final String correlationId;

  /**
   * used to complete the request future with timeout exception in case no response comes from service.
   */
  private static final ScheduledExecutorService delayer =
      ThreadFactory.singleScheduledExecutorService(ThreadFactory.SC_SERVICES_TIMEOUT);

  private static final ConcurrentMap<String, ServiceResponse> futures = new ConcurrentHashMap<>();

  private final CompletableFuture<Message> messageFuture;

  private ServiceResponse(String cid) {
    this.correlationId = cid;
    this.messageFuture = new CompletableFuture<>();
    futures.putIfAbsent(this.correlationId, this);
  }

  public static ServiceResponse correlationId(String cid) {
    return new ServiceResponse(cid);
  }

  /**
   * Correlation id of the request.
   * 
   * @return correlation id of the request.
   */
  public String correlationId() {
    return correlationId;
  }


  /**
   * handle a response message coming from the network.
   * 
   * @param message with correlation id.
   */
  public static void handleReply(Message message) {
    String correlationId = message.correlationId();
    ServiceResponse response = futures.remove(correlationId);
    if (response != null) {
      if (message.header(ServiceHeaders.EXCEPTION) == null) {
        response.complete(message);
      } else {
        LOGGER.error("cid [{}] remote service invoke respond with error message {}", correlationId, message);
        response.completeExceptionally(message.data());
      }
    }
  }

  /**
   * complete the expected future response successfully and apply the function.
   * 
   * @param message the response message for completing the future.
   */
  public void complete(final Message message) {
    if (message.header("exception") == null) {
      this.messageFuture.complete(message);
    } else {
      LOGGER.error("cid [{}] remote service invoke respond with error message {}", correlationId, message);
      this.messageFuture.completeExceptionally(message.data());
    }
  }

  /**
   * complete the expected future response exceptionally with a given exception.
   * 
   * @param exception that caused this future to complete exceptionally.
   */
  public void completeExceptionally(Throwable exception) {
    this.messageFuture.completeExceptionally(exception);
  }

  /**
   * future of this correlated response.
   * 
   * @return CompletableFuture for this response.
   */
  public CompletableFuture<Message> future() {
    return this.messageFuture;
  }


  public void withTimeout(final Duration timeout) {
    timeoutAfter(this.future(), timeout);
  }

  private static CompletableFuture<?> timeoutAfter(final CompletableFuture<?> resultFuture, Duration timeout) {

    if (!resultFuture.isDone()) {
      // schedule to terminate the target goal in future in case it was not done yet
      delayer.schedule(() -> {
        // by this time the target goal should have finished.
        if (!resultFuture.isDone()) {
          // target goal not finished in time so cancel it with timeout.
          resultFuture.completeExceptionally(new TimeoutException("expecting response reached timeout!"));
        }
      }, timeout.toMillis(), TimeUnit.MILLISECONDS);
    }
    return resultFuture;
  }

}
