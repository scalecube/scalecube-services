package io.scalecube.events.api;

import java.util.Collection;

public interface TopicRegistry {

  public void register(MessageSubscriber pub);

  public void register(MessagePublisher sub);
  
  public void unregister(MessageSubscriber pub);

  public void unregister(MessagePublisher sub);

  Collection<String> lookup(Topic topic);
}
