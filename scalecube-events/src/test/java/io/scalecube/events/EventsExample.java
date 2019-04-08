package io.scalecube.events;

import io.scalecube.events.aeron.AeronBroker;
import io.scalecube.events.aeron.AeronMessagePublisher;
import io.scalecube.events.aeron.AeronMessageSubscriber;
import io.scalecube.events.api.Destination;
import io.scalecube.events.api.Topic;

public class EventsExample {

  static Topic topic = Topic.name("topic-1");

  public static void main(String[] args) {
    Destination dest = new Destination("localhost", 13003, 13004);
    
    AeronBroker broker = new AeronBroker().server(dest);
    
    Topic topic = Topic.name("topic-1");
    
    AeronMessagePublisher pub = broker.publisher(topic);
    AeronMessagePublisher pub1 = broker.publisher(topic);

    AeronMessageSubscriber sub = broker.subscriber(topic);
    sub.add(dest);

    sub.listen()
        .subscribe(
            c -> {
              System.out.println(c);
            });

    
    pub.send("hello-1");
    pub1.send("hello-2");
    pub.send("hello-3");
    pub1.send("hello-4");
  }
}
