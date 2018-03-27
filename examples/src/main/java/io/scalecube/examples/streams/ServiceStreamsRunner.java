package io.scalecube.examples.streams;

import io.scalecube.examples.services.GreetingServiceImpl;
import io.scalecube.examples.services.stocks.SimpleQuoteService;
import io.scalecube.services.streams.ServiceStreams;
import io.scalecube.streams.StreamProcessors;
import io.scalecube.streams.StreamProcessors.ServerStreamProcessors;

public class ServiceStreamsRunner {

  public static void main(String[] args) {
    ServerStreamProcessors server = StreamProcessors.server().port(8000).build();

    ServiceStreams serviceStreams = new ServiceStreams(server);

    serviceStreams.createSubscriptions(new GreetingServiceImpl());
    serviceStreams.createSubscriptions(new SimpleQuoteService());

    server.bind().whenComplete((s, r) -> System.out.println(s));
  }
}
