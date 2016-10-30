package io.scalecube.services;

import io.scalecube.cluster.ICluster;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class RemoteServiceInstance implements ServiceInstance {
  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteServiceInstance.class);

  private final ICluster cluster;
  private final Address address;
  private final String memberId;
  private final Boolean isLocal;
  private final String[] tags;
  private final String qualifier;

  /**
   * RemoteServiceinstance constructor to initiate instance.
   * @param cluster to be used for instance context.
   * @param serviceReference service reference of this instance.
   */
  public RemoteServiceInstance(ICluster cluster, ServiceReference serviceReference) {
    this.qualifier = serviceReference.qualifier();
    this.cluster = cluster;
    this.address = serviceReference.address();
    this.memberId = serviceReference.memberId();
    this.tags = serviceReference.tags();
    this.isLocal = false;
  }

  @Override
  public String qualifier() {
    return qualifier;
  }

  private CompletableFuture<Message> futureInvokeMessage(final Message request) throws Exception {
    final CompletableFuture<Message> messageFuture = new CompletableFuture<>();

    final String correlationId = "rpc-" + UUID.randomUUID().toString();

    Message requestMessage = composeRequest(request, correlationId);
    // Listen response
    this.cluster.listen().filter(message -> {
      return correlationId.equals(message.correlationId());
    })  .first().subscribe(message -> {
      if (message.header("exception") == null) {
        messageFuture.complete(message);
      } else {
        LOGGER.error("cid [{}] remote service invoke respond with error message {}", correlationId, message);
        messageFuture.completeExceptionally(message.data());
      }
    });

    // check that send operation completed successfully else report an error
    CompletableFuture<Void> sendFuture = sendRemote(requestMessage);
    sendFuture.whenComplete((success, error) -> {
      if (error != null) {
        LOGGER.debug("cid [{}] send remote service request message failed {} , error {}",
            requestMessage.correlationId(),
            requestMessage, error);
        messageFuture.completeExceptionally(error);
      }
    });
    return messageFuture;
  }

  private <T> CompletableFuture<T> futureInvokeGeneric(final Message request) throws Exception {
    final CompletableFuture<T> messageFuture = new CompletableFuture<>();

    final String correlationId = "rpc-" + UUID.randomUUID().toString();

    Message requestMessage = composeRequest(request, correlationId);

    // Listen response
    this.cluster.listen().filter(message -> {
      return correlationId.equals(message.correlationId());
    })  .first().subscribe(message -> {
      if (message.header("exception") == null) {
        messageFuture.complete(message.data());
      } else {
        messageFuture.completeExceptionally(message.data());
      }
    });

    // check that send operation completed successfully else report an error
    CompletableFuture<Void> sendFuture = sendRemote(requestMessage);
    sendFuture.whenComplete((success, error) -> {
      if (error != null) {
        LOGGER.debug("cid [{}] send remote service request message failed {} , error {}",
            requestMessage.correlationId(),
            requestMessage, error);
        messageFuture.completeExceptionally(error);
      }
    });
    return messageFuture;
  }

  private CompletableFuture<Void> sendRemote(Message requestMessage) {
    final CompletableFuture<Void> messageFuture = new CompletableFuture<>();
    LOGGER.debug("cid [{}] send remote service request message {}", requestMessage.correlationId(), requestMessage);
    this.cluster.send(address, requestMessage, messageFuture);
    return messageFuture;
  }

  @Override
  public <T> Object invoke(Message request, Optional<ServiceDefinition> definition) throws Exception {

    // Try to call via messaging
    // Request message

    if (definition.get().returnType().equals(CompletableFuture.class)) {
      if (definition.get().parameterizedType().equals(Message.class)) {
        return futureInvokeMessage(request);
      } else {
        return futureInvokeGeneric(request);
      }
    } else {
      CompletableFuture<T> future = futureInvokeGeneric(request);
      Object obj = future.get();
      return obj;
    }
  }

  private Message composeRequest(Message request, final String correlationId) {

    Message requestMessage = Message.builder()
        .data(request.data())
        .header("service", qualifier())
        .qualifier(qualifier())
        .correlationId(correlationId)
        .build();

    return requestMessage;
  }

  @Override
  public String memberId() {
    return this.memberId;
  }

  public Address address() {
    return address;
  }

  @Override
  public String[] tags() {
    return tags;
  }

  @Override
  public Boolean isLocal() {
    return this.isLocal;
  }

  public boolean isReachable() {
    return cluster.member(this.memberId).isPresent();
  }

  @Override
  public String toString() {
    return "RemoteServiceInstance [address=" + address + ", memberId=" + memberId + ", isLocal=" + isLocal + ", tags="
        + Arrays.toString(tags) + "]";
  }
}
