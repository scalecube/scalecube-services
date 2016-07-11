package io.scalecube.services.examples;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Random;

import javax.annotation.Nonnull;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ICluster;
import io.scalecube.services.ServiceFabric;
import io.scalecube.transport.Message;

/**
 * @author Anton Kharenko
 */
public class NodeB {

  public static void main(String[] args) throws Exception {
    // Init cluster
    int incarnationId = new Random().nextInt();
    ICluster clusterB = Cluster.newInstance("B-" + incarnationId, 4002, "localhost:4001");
    clusterB.joinAwait();

    // Create service fabric
    ServiceFabric serviceFabricB = ServiceFabric.newInstance(clusterB);

    // Create service client
    ExampleService exampleServiceClient = serviceFabricB.createServiceClient(ExampleService.class);

    // Call service and print response each 3 seconds
    int iteration = 0;
    while (true) {
      System.out.println("~ Iteration #" + iteration++);

      // Request-response
      Message reqMsg = Message.builder().data(new HelloRequest("World")).build();
      ListenableFuture<Message> sayHelloResponseFuture = exampleServiceClient.sayHello(reqMsg);
      Futures.addCallback(sayHelloResponseFuture, new FutureCallback<Message>() {
        @Override
        public void onSuccess(Message respMsg) {
          System.out.println("~ Received 'sayHello' RPC success response: " + respMsg);
        }

        @Override
        public void onFailure(@Nonnull Throwable t) {
          System.out.println("~ Received 'sayHello' RPC failure response: " + t);
        }
      });

      // Wait for 3 seconds... node A can be started/stopped meanwhile
      Thread.sleep(3000);
    }
  }

}
