package io.scalecube.services;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nonnull;

import io.scalecube.cluster.ClusterMember;
import io.scalecube.cluster.ICluster;
import io.scalecube.transport.Message;
import rx.functions.Action1;
import rx.functions.Func1;

/**
 * @author Anton Kharenko
 */
public class ServiceDispatcher {

  private final ICluster cluster;
  private final ServiceRegistry serviceRegistry;

  public ServiceDispatcher(final ICluster cluster, final ServiceRegistry serviceRegistry) {
    this.cluster = cluster;
    this.serviceRegistry = serviceRegistry;

    cluster.listen().filter(new Func1<Message, Boolean>() {
      @Override
      public Boolean call(Message message) {
        return message.header("service") != null;
      }
    }).subscribe(new Action1<Message>() {
      @Override
      public void call(final Message message) {
        final String serviceName = message.header("service");
        final String serviceMethod = message.header("serviceMethod");

        // TODO: check if not null
        ServiceInstance serviceInstance = serviceRegistry.localServiceInstance(serviceName);

        try {
          Object result = serviceInstance.invoke(serviceMethod, message);

          if (result == null) {
            // Do nothing - fire and forget method
          } else if (result instanceof ListenableFuture) {
            ListenableFuture futureResult = (ListenableFuture)  result;
            Futures.addCallback(futureResult, new FutureCallback<Object>() {
              @Override
              public void onSuccess(Object result) {
                Message serviceResponseMsg = (Message) result;
                ClusterMember senderMember = cluster.membership().member(message.sender().id()); // TODO: API can be better
                Message responseMsg = Message.builder()
                    .fillWith(serviceResponseMsg)
                    .correlationId(message.correlationId())
                    .build();
                cluster.send(senderMember, responseMsg);
              }

              @Override
              public void onFailure(@Nonnull Throwable t) {
                t.printStackTrace();
              }
            });
          } else {
            // TODO: unsupported result type logic ?
            throw new IllegalArgumentException();
          }

        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
  }




}
