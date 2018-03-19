package io.scalecube.examples.streams;

import io.scalecube.streams.StreamProcessors;

/**
 * Server for the streams echo runner example.
 */
public class StreamsEchoServerRunner {

  /**
   * Main method
   */
  public static void main(String[] args) throws Exception {
    StreamProcessors streamProcessors = StreamProcessors.newStreamProcessors();

    streamProcessors.withListenAddress("192.168.1.3").bind().thenAccept(address -> {
      System.out.println("Listen on: " + address);
      streamProcessors.server(streamProcessor -> streamProcessor.listen()
          .filter(message -> message.getQualifier().equalsIgnoreCase("q/hello"))
          .subscribe(
              message -> {
                System.out.println(message);
                streamProcessor.onNext(message);
              },
              Throwable::printStackTrace,
              () -> {
                System.out.println("Ok, this client is completed, .. good bye then");
                streamProcessor.onCompleted();
              }));
    });

    Thread.currentThread().join();
  }
}
