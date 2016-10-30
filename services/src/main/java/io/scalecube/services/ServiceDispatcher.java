package io.scalecube.services;

import io.scalecube.cluster.ICluster;
import io.scalecube.transport.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class ServiceDispatcher {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceDispatcher.class);

  public ServiceDispatcher(final ICluster cluster, final IServiceRegistry registry) {

    cluster.listen().filter(message -> {
      return message.qualifier() != null;
    }).subscribe(message -> {

      ServiceInstance serviceInstance = registry.getLocalInstance(message.qualifier());

      try {
        Object result = serviceInstance.invoke(message, Optional.empty());

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

      }
    });
  }

}
