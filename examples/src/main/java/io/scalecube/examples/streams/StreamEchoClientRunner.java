package io.scalecube.examples.streams;

import io.scalecube.streams.StreamMessage;
import io.scalecube.streams.StreamProcessor;
import io.scalecube.streams.StreamProcessors;
import io.scalecube.transport.Address;

import java.util.stream.IntStream;

/**
 * Client for the streams echo runner example.
 */
public class StreamEchoClientRunner {

  /**
   * Main method
   */
  public static void main(String[] args) throws Exception {
    StreamProcessors streamProcessors = StreamProcessors.newStreamProcessors();

    StreamProcessor streamProcessor = streamProcessors.client(Address.from("192.168.1.3:5801"));

    streamProcessor.listen().subscribe(System.out::println, Throwable::printStackTrace);
    IntStream.rangeClosed(1, 1).forEach(i -> {
      streamProcessor.onNext(StreamMessage.withQualifier("q/hello").build());
    });
    streamProcessor.onCompleted();

    Thread.currentThread().join();
  }
}
