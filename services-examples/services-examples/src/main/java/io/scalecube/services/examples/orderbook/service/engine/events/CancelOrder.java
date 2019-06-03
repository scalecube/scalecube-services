package io.scalecube.services.examples.orderbook.service.engine.events;

public class CancelOrder {

  private Long orderId;
  private Long remainingQuantity;
  private Long size;

  public CancelOrder() {}

  /**
   * Create a new cancel order.
   *
   * @param orderId the resting order id
   * @param remainingQuantity the remaining quantity of the resting order
   * @param size the size
   */
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
