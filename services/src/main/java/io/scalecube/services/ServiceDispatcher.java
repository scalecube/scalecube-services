package io.scalecube.services;

import java.util.concurrent.CompletableFuture;

import io.scalecube.cluster.ICluster;
import io.scalecube.transport.Message;
import rx.functions.Action1;
import rx.functions.Func1;

public class ServiceDispatcher {

  private final ICluster cluster;
  private final IServiceRegistry registry;

  public ServiceDispatcher(final ICluster cluster, final IServiceRegistry registry) {
    this.cluster = cluster;
    this.registry = registry;

    cluster.listen().filter(new Func1<Message, Boolean>() {
      @Override
      public Boolean call(Message message) {
        return message.qualifier() != null;
      }
    }).subscribe(new Action1<Message>() {
      @Override
      public void call(final Message message) {

        ServiceInstance serviceInstance = registry.getLocalInstance(message.qualifier());

        try {
          Object result = serviceInstance.invoke(message, Object.class);

          if (result == null) {
            // Do nothing - fire and forget method
          } else if (result instanceof CompletableFuture) {
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
              } ;
            });
          } else { // this is a sync request response call
            Message responseMessage = Message.builder()
                .data(result)
                .correlationId(message.correlationId())
                .build();
            cluster.send(message.sender(), responseMessage);
          }

        } catch (Exception e) {
          cluster.send(message.sender(), Message.builder()
              .data(e)
              .header("exception", "true")
              .correlationId(message.correlationId())
              .build());
          e.printStackTrace();
        }
      }
    });
  }

}
