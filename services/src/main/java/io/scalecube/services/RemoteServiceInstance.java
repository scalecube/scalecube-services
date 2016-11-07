package io.scalecube.services;

import io.scalecube.cluster.ICluster;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

public class RemoteServiceInstance implements ServiceInstance {
  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteServiceInstance.class);

  private final ICluster cluster;
  private final Address address;
  private final String memberId;
  private final String serviceName;

  /**
   * Remote service instance constructor to initiate instance.
   * 
   * @param cluster to be used for instance context.
   * @param serviceReference service reference of this instance.
   */
  public RemoteServiceInstance(ICluster cluster, ServiceReference serviceReference) {
    this.serviceName = serviceReference.serviceName();
    this.cluster = cluster;
    this.address = serviceReference.address();
    this.memberId = serviceReference.memberId();
  }

  @Override
  public String serviceName() {
    return serviceName;
  }

  @Override
  public Object invoke(Message request, ServiceDefinition definition) throws Exception {
    Preconditions.checkArgument(definition != null, "Service definition can't be null");

    // Try to call via messaging
    // Request message
    if (definition.returnType().equals(CompletableFuture.class)) {
      if (definition.parametrizedType().equals(Message.class)) {
        return futureInvoke(request, message -> message);
      } else {
        return futureInvoke(request, message -> message.data());
      }
    } else {
      throw new UnsupportedOperationException("Method: " + definition.method() + " must return CompletableFuture");
    }
  }


  private CompletableFuture<Object> futureInvoke(final Message request, Function<Message, Object> fn) throws Exception {
    final CompletableFuture<Object> messageFuture = new CompletableFuture<>();

    final String correlationId = "rpc-" + generateId();

    Message requestMessage = composeRequest(request, correlationId);
    // Listen response
    this.cluster.listen()
        .filter(message -> correlationId.equals(message.correlationId()))
        .first().subscribe(message -> {
          if (message.header("exception") == null) {
            messageFuture.complete(fn.apply(message));
          } else {
            System.out.println("RESPONSE: " + message.data());
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

  private String generateId() {
    return new UUID(ThreadLocalRandom.current().nextLong(), ThreadLocalRandom.current().nextLong()).toString();
  }

  private CompletableFuture<Void> sendRemote(Message requestMessage) {
    final CompletableFuture<Void> messageFuture = new CompletableFuture<>();
    LOGGER.debug("cid [{}] send remote service request message {}", requestMessage.correlationId(), requestMessage);
    this.cluster.send(address, requestMessage, messageFuture);
    return messageFuture;
  }

  private Message composeRequest(Message request, final String correlationId) {
    return Message.withData(request.data())
        .header("service", serviceName)
        .qualifier(serviceName)
        .correlationId(correlationId)
        .build();
  }

  @Override
  public String memberId() {
    return this.memberId;
  }

  public Address address() {
    return address;
  }

  @Override
  public Boolean isLocal() {
    return false;
  }

  public boolean isReachable() {
    return cluster.member(this.memberId).isPresent();
  }

  @Override
  public String toString() {
    return "RemoteServiceInstance [address=" + address
        + ", memberId=" + memberId
        + "]";
  }
}
