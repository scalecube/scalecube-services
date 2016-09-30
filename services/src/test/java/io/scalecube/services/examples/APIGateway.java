package io.scalecube.services.examples;

import javax.annotation.Nonnull;

import org.rapidoid.http.Req;
import org.rapidoid.io.IO;
import org.rapidoid.setup.On;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ICluster;
import io.scalecube.services.Microservices;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;


public class APIGateway {

  public static void main(String[] args) throws Exception {

    // Init cluster
    ICluster clusterB = Cluster.joinAwait(Address.from("localhost:8000"));

    // Create service fabric
    Microservices microservices = Microservices.newInstance(clusterB);

    // Create service client
    AdSelectorService service = microservices.createProxy(AdSelectorService.class);

    // initalize rapidoid HTTP listener
    On.port(8889).req(req -> {
      selectAd(service, req.async());
      return req;
    });
  }

  private static String selectAd(AdSelectorService service, Req req) {
    // Request-response
    Message reqMsg = Message.builder().data(new AdRequest("zone id = 4432453")).build();

    ListenableFuture<Message> future = service.selectAd(reqMsg);
    if (future != null) {
      Futures.addCallback(future, new FutureCallback<Message>() {
        @Override
        public void onSuccess(Message respMsg) {
          IO.write(req.response().out(), respMsg.toString());
          req.done();
          System.out.println("~ Received 'ad request' RPC success response: " + respMsg);
        }

        @Override
        public void onFailure(@Nonnull Throwable t) {
          IO.write(req.response().out(), "~ Received 'ad request' RPC failure response: " + t);
          req.done();
          System.out.println("~ Received 'ad request' RPC failure response: " + t);
        }
      });
    }
    return null;
  }

}
