package io.scalecube.streams.codec;

import io.scalecube.streams.ServerStreamProcessors;
import io.scalecube.streams.StreamMessage;
import io.scalecube.streams.StreamProcessor;
import io.scalecube.streams.StreamProcessors;
import io.scalecube.transport.Address;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class ServiceCaller {

  static class StringHolder {
    String payload;

    public StringHolder() {}

    public StringHolder(String s) {
      payload = s;
    }

    @Override
    public String toString() {
      return "StringHolder{" +
          "payload='" + payload + '\'' +
          '}';
    }
  }
  static class Service1 {

    CompletableFuture<StringHolder> call(StringHolder req) {
      CompletableFuture<StringHolder> resp = new CompletableFuture<>();
      resp.complete(new StringHolder(req + ":" + req));
      return resp;
    }
  }

  public static void main(String[] args) throws InterruptedException, IOException {
    JsonMessageCodec codec = new JsonMessageCodec();
    Service1 service = new Service1();

    ServerStreamProcessors serverStreamProcessors = StreamProcessors.newServer();
    serverStreamProcessors.listen().subscribe(sp -> sp.listen().subscribe(streamMessage -> {
      try {
        StreamMessage req = codec.decode(streamMessage, StringHolder.class);
        System.out.println("Server Rcvd: " + req.data());
        StreamMessage response = StreamMessage.from(req).data(service.call((StringHolder) req.data()).get()).build();
        sp.onNext(codec.encode(StreamMessage.from(response).build()));
      } catch (Throwable e) {
        e.printStackTrace();
        sp.onError(e);
        return;
      }
      sp.onCompleted();
    }, t -> t.printStackTrace()));

    Address address = serverStreamProcessors.bindAwait();
    System.out.println("Started server on " + address);

    // Client
    StreamProcessor client = StreamProcessors.newClient().create(address);
    client.listen().subscribe(sr -> {
      StreamMessage sr1 = sr;
      try {
        StreamMessage response = codec.decode(sr1, StringHolder.class);
        System.out.println("Client Rcvd: " + response);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }, t -> t.printStackTrace());

    StreamMessage toSend =
        codec.encode(StreamMessage.builder().qualifier("qual").data(new StringHolder("hello")).build());
    client.onNext(toSend);
    System.out.println("Client Sent: " + toSend);
    Thread.currentThread().join();
  }
}
