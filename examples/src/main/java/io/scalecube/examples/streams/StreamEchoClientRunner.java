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
   * Main method.
   */
  public static void main(String[] args) throws Exception {
    StreamProcessors.ClientStreamProcessors client = StreamProcessors.client().build();

    StreamProcessor sp = client.create(Address.from("localhost:5801"));

    sp.listen().subscribe(
        System.out::println,
        Throwable::printStackTrace,
        () -> System.out.println("Done with client"));

    IntStream.rangeClosed(1, 5).forEach(i -> sp.onNext(StreamMessage.builder().qualifier("q/hello").build()));
    sp.onCompleted();

    Thread.currentThread().join();
  }
}
