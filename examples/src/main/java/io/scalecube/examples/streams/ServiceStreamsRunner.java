package io.scalecube.examples.streams;

import io.scalecube.examples.services.GreetingServiceImpl;
import io.scalecube.services.streams.ServiceStreams;
import io.scalecube.streams.StreamProcessors;
import io.scalecube.streams.StreamProcessors.ServerStreamProcessors;

import rx.Subscription;

import java.util.List;

public class ServiceStreamsRunner {
  public static void main(String[] args) {
    ServerStreamProcessors sp = StreamProcessors.server().port(8000).build();
    
    ServiceStreams.builder().server(sp).build().from(new GreetingServiceImpl());
    sp.bind().whenComplete((s,r)->{
      System.out.println(s);
    });
  }
}

