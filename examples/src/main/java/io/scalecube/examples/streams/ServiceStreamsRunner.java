package io.scalecube.examples.streams;

import io.scalecube.examples.services.GreetingServiceImpl;
import io.scalecube.services.streams.ServiceStreams;

public class ServiceStreamsRunner {
  public static void main(String[] args) {
    ServiceStreams.builder().build().from(new GreetingServiceImpl());
  }
}

