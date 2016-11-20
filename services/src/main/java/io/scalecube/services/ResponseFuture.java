package io.scalecube.services;

import io.scalecube.transport.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

/**
 * ResponseFuture handles the response of a service request based on correlationId. it holds a mapping between
 * correlationId to CompleteableFuture so when a response message is returned from service endpoint the future is
 * completed with success or error.
 */
public class ResponseFuture {

  private static final Logger LOGGER = LoggerFactory.getLogger(ResponseFuture.class);

  private static final ConcurrentMap<String, ResponseFuture> futures = new ConcurrentHashMap<>();

  private final String correlationId;
  private final Function<Message, Object> function;
  private final CompletableFuture<Object> messageFuture;

  public ResponseFuture(Function<Message, Object> fn) {
    this.correlationId = generateId();
    this.function = fn;
    this.messageFuture = new CompletableFuture<>();
    futures.putIfAbsent(this.correlationId, this);
  }

  /**
   * return a pending ResponseFuture by given correlationId.
   * 
   * @param correlationId or the request.
   * @return ResponseFuture pending completion.
   */
  public static ResponseFuture get(String correlationId) {
    return futures.get(correlationId);
  }

  /**
   * complete the expected future response successfully and apply the function.
   * 
   * @param message the response message for completing the future.
   */
  public void complete(Message message) {
    if (message.header("exception") == null) {
      messageFuture.complete(function.apply(message));
    } else {
      LOGGER.error("cid [{}] remote service invoke respond with error message {}", correlationId, message);
      messageFuture.completeExceptionally(message.data());
    }
    futures.remove(this.correlationId);
  }

  /**
   * complete the expected future response exceptionally with a given exception.
   * 
   * @param exception that caused this future to complete exceptionally.
   */
  public void completeExceptionally(Throwable exception) {
    messageFuture.completeExceptionally(exception);
    futures.remove(this.correlationId);
  }

  /**
   * future of this correlated response.
   * 
   * @param CompletableFuture for this response.
   */
  public CompletableFuture<Object> future() {
    return messageFuture;
  }

  /**
   * Correlation id of the request.
   * 
   * @param String correlation id of the request.
   */
  public String correlationId() {
    return correlationId;
  }

  private String generateId() {
    return new UUID(ThreadLocalRandom.current().nextLong(), ThreadLocalRandom.current().nextLong()).toString();
  }

}
