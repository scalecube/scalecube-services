package io.scalecube.services;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.SettableFuture;

import io.scalecube.cluster.ICluster;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;
import rx.Subscription;
import rx.functions.Action1;
import rx.functions.Func1;

public class RemoteServiceInstance implements ServiceInstance {

  @Override
  public String toString() {
    return "RemoteServiceInstance [cluster=" + cluster + ", serviceReference=" + serviceReference + ", address="
        + address + ", memberId=" + memberId + "]";
  }

  private final ICluster cluster;
  private final ServiceReference serviceReference;
  private final Address address;
  private final String memberId;
  
  public RemoteServiceInstance(ICluster cluster, ServiceReference serviceReference) {
    this.cluster = cluster;
    this.serviceReference = serviceReference;
    // Send request
    address = cluster.member(serviceReference.memberId()).get().address();
    this.memberId = serviceReference.memberId();
  }

  @Override
  public String serviceName() {
    return serviceReference.serviceName();
  }

  public ServiceReference serviceReference(){
    return this.serviceReference;
  }
  
  @Override
  public Object invoke(String methodName, Message request) throws Exception {
    // Try to call via messaging
    // Request message
    final String correlationId = "rpc-" + UUID.randomUUID().toString();
    final SettableFuture<Object> responseFuture = SettableFuture.create();
    Message requestMessage = Message.builder()
        .data(request.data())
        .header("service", serviceName())
        .header("serviceMethod", methodName)
        .correlationId(correlationId)
        .build();
    
    final AtomicReference<Subscription>  subscriber = new AtomicReference<Subscription>(null);
    // Listen response
    subscriber.set(cluster.listen().filter(new Func1<Message, Boolean>() {
      @Override
      public Boolean call(Message message) {
        return correlationId.equals(message.correlationId());
      }
    }).subscribe(new Action1<Message>() {
      @Override
      public void call(Message message) {
        responseFuture.set(message);
        subscriber.get().unsubscribe();
      }
    }));
    
    cluster.send(address, requestMessage);

    return responseFuture;
  }

  @Override
  public String memberId() {
    return this.memberId;
  }
}
