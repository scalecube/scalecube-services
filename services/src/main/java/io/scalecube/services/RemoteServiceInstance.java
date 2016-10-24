package io.scalecube.services;

import java.util.Arrays;
import java.util.UUID;

import com.google.common.util.concurrent.SettableFuture;

import io.scalecube.cluster.ICluster;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;

public class RemoteServiceInstance implements ServiceInstance {


  private final ICluster cluster;
  private final Address address;
  private final String memberId;
  private final Boolean isLocal;
  private final String[] tags;
  private final String qualifier;

  public RemoteServiceInstance(ICluster cluster, ServiceReference serviceReference) {
    this.qualifier = serviceReference.serviceName();
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

  @Override
  public Object invoke(Message request) throws Exception {
    // Try to call via messaging
    // Request message
    final String correlationId = "rpc-" + UUID.randomUUID().toString();
    final SettableFuture<Object> responseFuture = SettableFuture.create();

    Message requestMessage = Message.builder()
        .data(request.data())
        .qualifier(qualifier())
        .correlationId(correlationId)
        .build();

    // Listen response
    cluster.listen().filter(message -> {
      return correlationId.equals(message.correlationId());
    }).subscribe(message -> {
      if (message.header("exception") != null) {
        responseFuture.setException(message.data());
      } else if (isFutureClassTypeEqualsMessage(responseFuture)) {
        responseFuture.set(message);
      } else {
        responseFuture.set(message.data());
      }
    });

    cluster.send(address, requestMessage);

    return responseFuture;
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

  private boolean isFutureClassTypeEqualsMessage(final SettableFuture<Object> responseFuture) {
    return responseFuture.getClass().getGenericSuperclass().getClass().equals(Message.class);
  }

  
  @Override
  public String toString() {
    return "RemoteServiceInstance [address=" + address + ", memberId=" + memberId + ", isLocal=" + isLocal + ", tags=" + Arrays.toString(tags) + "]";
  }


}
