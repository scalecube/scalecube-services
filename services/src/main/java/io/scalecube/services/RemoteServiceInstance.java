package io.scalecube.services;

import com.google.common.util.concurrent.SettableFuture;

import java.util.Set;
import java.util.UUID;

import io.scalecube.cluster.ClusterMember;
import io.scalecube.cluster.ICluster;
import io.scalecube.transport.TransportHeaders;
import io.scalecube.transport.Message;
import rx.functions.Action1;
import rx.functions.Func1;

/**
 * @author Anton Kharenko
 */
public class RemoteServiceInstance implements ServiceInstance {

  private final ICluster cluster;
  private final ServiceReference serviceReference;

  public RemoteServiceInstance(ICluster cluster, ServiceReference serviceReference) {
    this.cluster = cluster;
    this.serviceReference = serviceReference;
  }

  @Override
  public String serviceName() {
    return serviceReference.serviceName();
  }

  @Override
  public Set<String> methodNames() {
    return serviceReference.methodNames();
  }

  @Override
  public Object invoke(String methodName, Message request) throws Exception {
    // Try to call via messaging
    // Request message
    final String correlationId = "rpc-" + UUID.randomUUID().toString();
    final SettableFuture<Object> responseFuture = SettableFuture.create();
    Message requestMessage = Message.builder()
        .fillWith(request)
        .header("service", serviceName())
        .header("serviceMethod", methodName)
        .correlationId(correlationId)
        .build();

    // Listen response
    cluster.listen().filter(new Func1<Message, Boolean>() {
      @Override
      public Boolean call(Message message) {
        return correlationId.equals(message.header(TransportHeaders.CORRELATION_ID));
      }
    }).subscribe(new Action1<Message>() {
      @Override
      public void call(Message message) {
        responseFuture.set(message);
      }
    });

    // Send request
    ClusterMember member = cluster.membership().member(serviceReference.memberId());
    cluster.send(member, requestMessage);

    return responseFuture;
  }
}
