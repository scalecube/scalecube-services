package io.scalecube.examples.streams;

import io.scalecube.streams.ClientStreamProcessors;
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
    ClientStreamProcessors client = StreamProcessors.newClient();

    StreamProcessor<StreamMessage, StreamMessage> sp = client.create(Address.from("localhost:8000"));

    sp.listen().subscribe(
        System.out::println,
        Throwable::printStackTrace,
        () -> System.out.println("Done with client"));

    IntStream.rangeClosed(1, 5)
        .forEach(i -> sp.onNext(StreamMessage.builder().qualifier("scalecube-greeting-service/greeting").build()));
    sp.onCompleted();

    Thread.currentThread().join();
  }
}
