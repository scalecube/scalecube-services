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
    Optional<ServiceInstance> serviceInstance = registry.getLocalInstance(message.qualifier(), message.method());
    try {
      if (serviceInstance.isPresent()) {
        Object result = serviceInstance.get().invoke(message, null);
        if (result != null) {
          if (result instanceof Throwable) {
            repleyWithError(message, Throwable.class.cast(result));
          } else if (result instanceof CompletableFuture) {
            handleComputable(cluster, message, result);
          } else { // this is a sync request response call
            handleSync(cluster, message, result);
          }
        }
      } else {
        repleyWithError(message,
            new IllegalStateException("no local service instance was found for service request: [" + message + "]"));
      }
    } catch (Exception ex) {
      repleyWithError(message, ex);
    }

  }

  private void repleyWithError(Message message, Throwable ex) {
    Message errorResponseMsg = Message.builder()
        .data(ex)
        .header("exception", "true")
        .correlationId(message.correlationId())
        .build();
    cluster.send(message.sender(), errorResponseMsg);
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
      } else {
        futureMessage = Message.builder()
            .data(error)
            .header("exception", "true")
            .correlationId(message.correlationId())
            .build();
      }
      cluster.send(message.sender(), futureMessage);
    });
  }

}
