package io.scalecube.services;

import io.scalecube.cluster.Cluster;
import io.scalecube.transport.Message;

import java.util.concurrent.CompletableFuture;

/**
 * handles the future invocation of a local service method.
 */
public class DispatchingFuture {

  /**
   * the original request this replay referees to.
   */
  private final Message request;

  /**
   * the instance of the relevant cluster.
   */
  private Cluster cluster;

  /**
   * create a method dispatching future with relevant original request.
   * 
   * @param cluster the cluster instance this future assigned to.
   * @param request the original request this response correlated to.
   * @return new instance of a dispatching future.
   */
  static DispatchingFuture from(Cluster cluster, Message request) {
    return new DispatchingFuture(cluster, request);
  }

  /**
   * private contractor use static method from.
   * 
   * @param cluster instance.
   * @param request original service request.
   */
  private DispatchingFuture(Cluster cluster, Message request) {
    this.request = request;
    this.cluster = cluster;
  }

  /**
   * complete this future with error or success.
   * 
   * @param value might be instance of Throwable or CompletableFuture future. if the value is CompletebleFuture then
   *        whenCompleted reply with a result to sender. if the value is Throwable reply with error to sender. if value
   *        is null it is just ignored - and no response sent to sender.
   */
  public void complete(Object value) {
    if (value instanceof Throwable) {
      completeExceptionally(Throwable.class.cast(value));
    } else if (value instanceof CompletableFuture<?>) {
      handleComputable(cluster, CompletableFuture.class.cast(value));
    } else if (value == null) {
      handleComputable(cluster, CompletableFuture.completedFuture(null));
    }
  }

  /**
   * complete this future and explicitly reply with a give Throwable to sender.
   * 
   * @param error given error to reply with.
   */
  public void completeExceptionally(Throwable error) {
    Message errorResponseMsg = Message.builder()
        .data(error)
        .header(ServiceHeaders.SERVICE_RESPONSE, "Response")
        .header("exception", "true")
        .correlationId(request.correlationId())
        .build();
    cluster.send(request.sender(), errorResponseMsg);
  }


  private void handleComputable(final Cluster cluster, CompletableFuture<?> result) {
    result.whenComplete((success, error) -> {
      Message futureMessage = null;
      if (error == null) {
        if (success instanceof Message) {
          Message successMessage = (Message) success;
          futureMessage = composeResponse(successMessage.data());
        } else {
          futureMessage = composeResponse(success);
        }
      } else {
        completeExceptionally(error);
      }
      cluster.send(request.sender(), futureMessage);
    });
  }

  private Message composeResponse(Object data) {
    return Message.builder()
        .data(data)
        .header(ServiceHeaders.SERVICE_RESPONSE, "Response")
        .correlationId(request.correlationId())
        .build();
  }
}
