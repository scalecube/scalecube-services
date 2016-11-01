package io.scalecube.services;

import io.scalecube.cluster.ICluster;
import io.scalecube.transport.Message;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class ServiceDispatcher {

  private final ICluster cluster;
  private final IServiceRegistry registry;

  /**
   * ServiceDispatcher constructor to listen on incoming network service request.
   * 
   * @param cluster instance to listen on events.
   * @param registry service registry instance for dispatching.
   */
  public ServiceDispatcher(ICluster cluster, IServiceRegistry registry) {
    this.cluster = cluster;
    this.registry = registry;

    // Start listen messages
    cluster.listen()
        .filter(message -> message.qualifier() != null)
        .subscribe(this::onServiceRequest);
  }

  private void onServiceRequest(Message message) {
    ServiceInstance serviceInstance = registry.getLocalInstance(message.qualifier());
    // TODO [AK]: serviceInstance can be null, need to process this case or next call will throw NPE
    try {
      Object result = serviceInstance.invoke(message, Optional.empty());

      if (result != null) {
        if (result instanceof CompletableFuture) {
          handleComputable(cluster, message, result);
        } else { // this is a sync request response call
          handleSync(cluster, message, result);
        }
      }
    } catch (Exception e) {
      Message errorResponseMsg = Message.builder()
          .data(e)
          .header("exception", "true")
          .correlationId(message.correlationId())
          .build();
      cluster.send(message.sender(), errorResponseMsg);
    }
  }

  private void handleSync(final ICluster cluster, Message message, Object result) {
    Message responseMessage = Message.builder()
        .data(result)
        .correlationId(message.correlationId())
        .build();
    cluster.send(message.sender(), responseMessage);
  }

  private void handleComputable(final ICluster cluster, Message message, Object result) {
    CompletableFuture<?> futureResult = (CompletableFuture<?>) result;
 
    futureResult.whenComplete((success, error) -> {
      Message futureMessage = null;
      if (error == null) {
        if (success instanceof Message) {
          Message successMessage = (Message) success;
          futureMessage = Message.builder()
              .data(successMessage.data())
              .correlationId(message.correlationId())
              .build();
        } else {
          futureMessage = Message.builder()
              .data(success)
              .correlationId(message.correlationId())
              .build();
        }
        cluster.send(message.sender(), futureMessage);
      }
    });
  }

}
