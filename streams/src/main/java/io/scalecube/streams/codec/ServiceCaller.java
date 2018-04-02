package io.scalecube.streams.codec;

import io.scalecube.streams.ServerStreamProcessors;
import io.scalecube.streams.StreamMessage;
import io.scalecube.streams.StreamProcessor;
import io.scalecube.streams.StreamProcessors;
import io.scalecube.transport.Address;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class ServiceCaller {

  JsonMessageCodec jsonMessageCodec;

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
      resp.complete(new StringHolder(req + " " + req));
      return resp;
    }
  }

  public static void main(String[] args) throws InterruptedException {
    JsonMessageCodec codec = new JsonMessageCodec( /* remove */);
    ServerStreamProcessors serverStreamProcessors = StreamProcessors.newServer();
    serverStreamProcessors.listen().subscribe(sp -> sp.listen().subscribe(streamMessage -> {

      Object reqPayload = null;
      try {
        reqPayload = codec.readFrom(new ByteBufInputStream((ByteBuf) streamMessage.data()), StringHolder.class);
      } catch (IOException e) {
        e.printStackTrace();
        sp.onError(e);
        return;
      }
      StreamMessage req = StreamMessage.from(streamMessage).data(reqPayload).build();
      System.out.println("Rcvd : " + req.data());
      sp.onNext(req);
      // sp.onCompleted();
    }, t -> t.printStackTrace()));

    Address address = serverStreamProcessors.bindAwait();
    System.out.println("Started server on " + address);

    StreamProcessor client = StreamProcessors.newClient().create(address);
    client.listen().subscribe(sr -> System.out.println(sr), t -> t.printStackTrace());

    ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
    try {
      codec.writeTo(new ByteBufOutputStream(buffer), new StringHolder("hello"));
    } catch (IOException e) {
      System.out.println("Client encountered error  while serializing");
      return;
    }
    client.onNext(StreamMessage.builder().qualifier("qual").data(buffer).build());
    System.out.println("Sent");
    Thread.currentThread().join();
  }
}
