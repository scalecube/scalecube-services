package io.scalecube.transport;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

public class Topic<T> {

  class Subscription {

    private Subscription() {
      // do not initialize.
    }

    private String id;

    public Subscription(String id) {
      this.id = id;
    }

    public String id() {
      return this.id;
    }
  }

  private final ConcurrentMap<String, Function<Message, Void>> subscribers;

  private final ConcurrentMap<String, Function<Message, Boolean>> filters;

  private Topic() {
    subscribers = new ConcurrentHashMap<String, Function<Message, Void>>();
    filters = new ConcurrentHashMap<String, Function<Message, Boolean>>();
  }

  public static <T> Topic<T> create() {
    return new Topic<>();
  }


  /**
   * on next message.
   * 
   * @param onNext event.
   */
  public void onNext(Message onNext) {

    // avoid concurrent modification and copy map for iteration.
    ConcurrentMap<String, Function<Message, Void>> map = copy();

    map.entrySet().stream().forEach(item -> {
      Function<Message, Boolean> filter = filters.get(item.getKey());
      if (filter != null) {
        if (filter.apply(onNext)) {
          item.getValue().apply(onNext);
        }
      } else {
        item.getValue().apply(onNext);
      }
    });

  }

  public Topic<T> filter(String name, Function<Message, Boolean> filter) {
    filters.putIfAbsent(name, filter);
    return this;
  }

  /**
   * subscribe to the topic.
   * 
   * @param function of the subscriber.
   */
  public Subscription subscribe(final Function<Message, Void> function) {
    String name = UUID.randomUUID().toString();
    subscribers.putIfAbsent(UUID.randomUUID().toString(), function);
    return new Subscription(name);
  }

  /**
   * subscribe to the topic.
   * 
   * @filter function before executing the subscription if true than execute
   * @param function of the subscriber.
   * @return subscription id
   */
  public Subscription subscribe(Function<Message, Boolean> filter, final Function<Message, Void> function) {
    String name = UUID.randomUUID().toString();
    subscribers.putIfAbsent(name, function);
    filters.putIfAbsent(name, filter);
    return new Subscription(name);
  }

  /**
   * subscribe to the topic.
   * 
   * @param subscription of the subscriber.
   */
  public void unsubscribe(Subscription subscription) {
    subscribers.remove(subscription.id);
    filters.remove(subscription.id);
  }

  private ConcurrentHashMap<String, Function<Message, Void>> copy() {
    ConcurrentHashMap<String, Function<Message, Void>> map =
        new ConcurrentHashMap<String, Function<Message, Void>>();
    map.putAll(subscribers);
    return map;
  }

  public boolean contains(String name) {
    // TODO Auto-generated method stub
    return false;
  }

}
