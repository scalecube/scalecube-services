package io.scalecube.services;

import io.scalecube.transport.Message;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class Subscriptions {

  final ConcurrentMap<String, ServiceSubscription> subscriptionsMap = new ConcurrentHashMap<>();

  public Subscriptions(Microservices microservices) {
   
    microservices.cluster().listenMembership()
        .filter(predicate -> predicate.isRemoved())
        .subscribe(onNext -> {
          subscriptionsMap.values().stream()
              .filter(action -> action.memberId().equals(onNext.member().id()))
              .collect(Collectors.toList())
              
              .forEach(sub -> {
                sub.unsubscribe();
                subscriptionsMap.remove(sub.id());
              });;
        });

    microservices.sender().listen().filter(request -> request.headers().containsKey(ServiceHeaders.DISPATCHER_SERVICE))
        .filter(request -> request.header(ServiceHeaders.DISPATCHER_SERVICE).equals(ServiceHeaders.UNSUBSCIBE))
        .subscribe(onNext -> onUnsubscribed(onNext));
  }

  private void onUnsubscribed(Message request) {
    this.unsubscribe(request.correlationId());
  }

  public void unsubscribe(String id) {
    ServiceSubscription sub = subscriptionsMap.remove(id);
    if (sub != null) {
      sub.unsubscribe();
    }
  }

  public void put(String id, ServiceSubscription serviceSubscription) {
    subscriptionsMap.putIfAbsent(id, serviceSubscription);
  }

  public boolean contains(String id) {
    return subscriptionsMap.containsKey(id);
  }
}
