package io.scalecube.examples.orderbook.service.engine.events;

public class CancelOrder {

  Long orderId, remainingQuantity, size;

  public CancelOrder() {};

  public CancelOrder(Long orderId, Long remainingQuantity, Long size) {
    this.orderId = orderId;
    this.remainingQuantity = remainingQuantity;
    this.size = size;
  }

  public Long orderId() {
    return orderId;
  }

  public Long remainingQuantity() {
    return remainingQuantity;
  }

  public Long size() {
    return size;
  }

}
