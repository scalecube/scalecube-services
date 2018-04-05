package io.scalecube.examples.streams;

import io.scalecube.streams.ServerStreamProcessors;
import io.scalecube.streams.StreamMessage;
import io.scalecube.streams.StreamProcessor;
import io.scalecube.streams.StreamProcessors;

/**
 * Server for the streams echo runner example.
 */
public class StreamsEchoServerRunner {

  /**
   * Main method.
   */
  public static void main(String[] args) throws Exception {
    ServerStreamProcessors server = StreamProcessors.newServer();

    server.bind().thenAccept(address -> {
      System.out.println("Listen on: " + address);
      server.listen().subscribe(sp -> ((StreamProcessor<StreamMessage, StreamMessage>)sp).listen()
          .filter(message -> message.qualifier().equalsIgnoreCase("q/hello"))
          .subscribe(
              message -> {
                System.out.println(message);
                sp.onNext(message);
              },
              Throwable::printStackTrace,
              () -> {
                System.out.println("Client is done, ok, finish with server .. good bye then");
                sp.onCompleted();
              }));
    });

    Thread.currentThread().join();
  }
}
