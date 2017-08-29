package io.scalecube.services;

import io.scalecube.transport.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class Subscriptions {
  private static final Logger LOGGER = LoggerFactory.getLogger(Subscriptions.class);

  private final ConcurrentMap<String, ServiceSubscription> subscriptionsMap = new ConcurrentHashMap<>();

  /**
   * map remote subscriptions to handle cases where subscriptions needs to automatically unsubscribe. listen on cluster.
   * changes and in case member id is removed from cluster remove the member subscriptions. listen on unsubscribe
   * requests and unsubscribe in case remote subscription is unsubscribed.
   * 
   * @param microservices of the instance to listen for events.
   */
  public Subscriptions(Microservices microservices) {

    microservices.cluster().listenMembership()
        .filter(predicate -> predicate.isRemoved())
        .subscribe(onNext -> {
          subscriptionsMap.values().stream()
              .filter(action -> action.memberId().equals(onNext.member().id()))
              .collect(Collectors.toList())

              .forEach(subscription -> {
                subscription.unsubscribe();
                subscriptionsMap.remove(subscription.id());
                LOGGER.info("Member removed removing subscription {}", subscription);
              });
        });

    microservices.sender().listen().filter(request -> request.headers().containsKey(ServiceHeaders.OBSERVER))
        .filter(request -> request.header(ServiceHeaders.OBSERVER).equals(ServiceHeaders.UNSUBSCIBE))
        .subscribe(onNext -> onUnsubscribed(onNext));
  }

  /**
   * Unsubscribe and remove the subscription for a given correlation id.
   * 
   * @param id correlation id of the subscription.
   */
  public void unsubscribe(String id) {
    ServiceSubscription sub = subscriptionsMap.remove(id);
    if (sub != null) {
      sub.unsubscribe();
      LOGGER.info("Removing subscription: {}", sub);
    }
  }

  private void onUnsubscribed(Message request) {
    this.unsubscribe(request.correlationId());
  }

  public void put(String id, ServiceSubscription serviceSubscription) {
    subscriptionsMap.putIfAbsent(id, serviceSubscription);
  }

  public boolean contains(String id) {
    return subscriptionsMap.containsKey(id);
  }
}
