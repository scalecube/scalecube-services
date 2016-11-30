package io.scalecube.services;

import io.scalecube.cluster.ICluster;
import io.scalecube.transport.ITransport;
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
  private final ITransport transport;

  /**
   * create a method dispatching future with relevant original request.
   * 
   * @param transport the cluster instance this future assigned to.
   * @param request the original request this response correlated to.
   * @return new instance of a dispatching future.
   */
  static DispatchingFuture from(ITransport transport, Message request) {
    return new DispatchingFuture(transport, request);
  }

  /**
   * private contractor use static method from.
   * @param transport instance.
   * @param request original service request.
   */
  private DispatchingFuture(ITransport transport, Message request) {
    this.request = request;
    this.transport = transport;
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
      handleComputable(CompletableFuture.class.cast(value));
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
    transport.send(request.sender(), errorResponseMsg);
  }


  private void handleComputable(CompletableFuture<?> result) {
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
      transport.send(request.sender(), futureMessage);
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
