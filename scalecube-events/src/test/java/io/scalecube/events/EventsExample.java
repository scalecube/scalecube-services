package io.scalecube.events;

import io.scalecube.events.aeron.AeronMessagePublisher;
import io.scalecube.events.aeron.AeronMessageSubscriber;
import io.scalecube.events.api.Destination;
import io.scalecube.events.api.Topic;

public class EventsExample {

  static Topic topic = Topic.name("topic-1");

  public static void main(String[] args) {

    Destination dest = new Destination("localhost", 13003, 13004);
    
    AeronMessagePublisher pub = new AeronMessagePublisher(dest);
    AeronMessageSubscriber sub = new AeronMessageSubscriber(topic);
    sub.add(dest);
      
    sub.listen().subscribe(c->{
      System.out.println(c ); 
    });
    
    pub.send("hello");
    pub.send("hello");
    pub.send("hello");
    pub.send("hello");
    
  }
}
