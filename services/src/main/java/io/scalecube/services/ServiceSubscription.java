package io.scalecube.services;

import rx.Subscription;

public class ServiceSubscription {

  private String id;
  private Subscription subscription;
  private String memberId;

  public ServiceSubscription(String id, Subscription subscription, String memberId) {
    this.id = id;
    this.subscription = subscription;
    this.memberId = memberId;
  }

  public String memberId() {
    return this.memberId;
  }

  public void unsubscribe() {
    this.subscription.unsubscribe();
  }

  public String id() {
    return id;
  }

  @Override
  public String toString() {
    return "ServiceStreamMethodInvoker [id=" + id + ", subscription=" + subscription + ", memberId=" + memberId + "]";
  }
}
