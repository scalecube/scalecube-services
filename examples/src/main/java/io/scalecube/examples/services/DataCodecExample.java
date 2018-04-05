package io.scalecube.examples.services;

import io.scalecube.streams.codec.StreamMessageDataCodecImpl;
import io.scalecube.streams.ServerStreamProcessors;
import io.scalecube.streams.StreamMessage;
import io.scalecube.streams.StreamProcessor;
import io.scalecube.streams.StreamProcessors;
import io.scalecube.streams.codec.StreamMessageDataCodec;
import io.scalecube.transport.Address;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * Example of using deserialization and serialization of payload in StreamMessage.
 */
public class DataCodecExample {

  static StreamMessageDataCodec codec = new StreamMessageDataCodecImpl();
  static Service1 service = new Service1();

  /**
   * Runner start.
   * 
   * @param args Arg to pass
   * @throws InterruptedException in case of exception
   * @throws IOException in case of exception
   */
  public static void main(String[] args) throws InterruptedException, IOException {

    ServerStreamProcessors serverStreamProcessors = StreamProcessors.newServer();
    serverStreamProcessors.listen().subscribe(sp -> sp.listen().subscribe(fromConsumer -> {
      // Service Provider logic:
      // Deserialize request data -> pass data to Service -> serialize response data
      try {
        StreamMessage req = codec.decodeData((StreamMessage) fromConsumer, StringHolder.class);
        System.out.println("Server Rcvd: " + req.data());
        StreamMessage afterService =
            StreamMessage.from(req).data(service.doubleEcho((StringHolder) req.data()).get()).build();
        StreamMessage toConsumer = codec.encodeData(StreamMessage.from(afterService).build());
        sp.onNext(toConsumer);
      } catch (Throwable e) {
        System.err.println("Server Err:" + e.getLocalizedMessage());
        sp.onError(e);
        return;
      }
      sp.onCompleted();
    }));

    Address address = serverStreamProcessors.bindAwait();
    System.out.println("Started server on " + address);

    // Client
    StreamProcessor client = StreamProcessors.newClient().create(address);
    client.listen().subscribe(sr -> {
      StreamMessage response = codec.decodeData((StreamMessage) sr, StringHolder.class);
      System.out.println("Client Rcvd: " + response.data());
    });

    StreamMessage toSend =
        codec.encodeData(StreamMessage.builder().qualifier("qual").data(new StringHolder("hello")).build());
    client.onNext(toSend);
    System.out.println("Client Sent: " + toSend.data());
    Thread.currentThread().join();
  }

  static class StringHolder {
    String payload;

    public StringHolder() {}

    public StringHolder(String payload) {
      this.payload = payload;
    }

    @Override
    public String toString() {
      return "StringHolder{"
          + "payload='" + payload + '\''
          + '}';
    }
  }

  static class Service1 {
    CompletableFuture<StringHolder> doubleEcho(StringHolder req) {
      CompletableFuture<StringHolder> resp = new CompletableFuture<>();
      resp.complete(new StringHolder(req.payload + ":" + req.payload));
      return resp;
    }
  }

}
