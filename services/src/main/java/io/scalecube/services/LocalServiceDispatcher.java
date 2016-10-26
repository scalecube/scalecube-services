package io.scalecube.services;

import java.util.concurrent.CompletableFuture;

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
            Object result = serviceInstance.invoke(message, Object.class);

            if (result == null) {
              // Do nothing - fire and forget method
            } else if (result instanceof CompletableFuture) {
              CompletableFuture<?> futureResult = (CompletableFuture<?>) result;

              futureResult.whenComplete((success, error) -> {
                if (error == null) {
                  Message serviceResponseMsg = (Message) result;
                  Message responseMsg = Message.builder().data(serviceResponseMsg)
                      .correlationId(message.correlationId()).build();
                  cluster.send(message.sender(), responseMsg);
                } else {
                  Message responseMsg = Message.builder()
                      .data(error)
                      .correlationId(message.correlationId())
                      .header("exception", "true")
                      .build();
                  cluster.send(message.sender(), responseMsg);
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
