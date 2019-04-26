package io.scalecube.events;

import io.scalecube.events.api.MessageSubscriber;
import io.scalecube.events.api.MessagePublisher;
import io.scalecube.events.api.Topic;
import io.scalecube.events.api.TopicRegistry;
import java.util.Collection;
import reactor.core.publisher.TopicProcessor;

public class DefaultTopicRegistry implements TopicRegistry {

  TopicProcessor topic = TopicProcessor.create();
  
  @Override
  public Collection<String> lookup(Topic t) {
    return null;
  }
  
  @Override
  public void register(MessageSubscriber pub) {
    
  }

  @Override
  public void register(MessagePublisher sub) {
    
  }

  @Override
  public void unregister(MessageSubscriber pub) {
    
  }

  @Override
  public void unregister(MessagePublisher sub) {
    
  }
  
}
