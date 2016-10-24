package io.scalecube.services;

import javax.annotation.Nonnull;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import io.scalecube.cluster.ICluster;
import io.scalecube.transport.Message;

public class LocalServiceDispatcher {

  private final ICluster cluster;
  private final IServiceRegistry registry;

  public LocalServiceDispatcher(final ICluster cluster, final IServiceRegistry registry) {
    this.cluster = cluster;
    this.registry = registry;

    this.cluster.listen().filter(
        message -> message.header("service") != null)
        .subscribe(message -> {
          final String serviceName = message.header("service");
          ServiceInstance serviceInstance = this.registry.getLocalInstance(serviceName);

          try {
            Object result = serviceInstance.invoke(message);

            if (result == null) {
              // Do nothing - fire and forget method
            } else if (result instanceof ListenableFuture) {
              ListenableFuture<Message> futureResult = (ListenableFuture<Message>) result;
              Futures.addCallback(futureResult, new FutureCallback<Object>() {
                @Override
                public void onSuccess(Object result) {
                  Message serviceResponseMsg = (Message) result;
                  Message responseMsg = Message.builder().data(serviceResponseMsg)
                      .correlationId(message.correlationId()).build();
                  cluster.send(message.sender(), responseMsg);
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
        });
  }

}
