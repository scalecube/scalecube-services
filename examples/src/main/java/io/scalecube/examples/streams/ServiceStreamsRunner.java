package io.scalecube.examples.streams;

import io.scalecube.examples.services.GreetingServiceImpl;
import io.scalecube.examples.services.stocks.SimpleQuoteService;
import io.scalecube.services.streams.ServiceStreams;
import io.scalecube.streams.ServerStreamProcessors;
import io.scalecube.streams.StreamProcessors;

public class ServiceStreamsRunner {

  /**
   * main running server listener for stream.
   * 
   * @param args noop.
   */
  public static void main(String[] args) {
    ServerStreamProcessors server = StreamProcessors.newServer().port(8000);

    ServiceStreams serviceStreams = new ServiceStreams(server);

    serviceStreams.createSubscriptions(new GreetingServiceImpl());
    serviceStreams.createSubscriptions(new SimpleQuoteService());

    server.bind().whenComplete((value, ex) -> System.out.println(value));
  }
}
