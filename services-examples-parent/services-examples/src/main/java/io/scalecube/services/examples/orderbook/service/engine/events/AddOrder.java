package io.scalecube.services.examples.orderbook.service.engine.events;

public class AddOrder {

  long orderId;
  Side side;
  long price;
  long remainingQuantity;

  public AddOrder() {}

  /**
   * Create a new Add order.
   *
   * @param orderId the Id of the incoming order
   * @param side the side of this level (either {@link Side#BUY} or {@link Side#SELL})
   * @param price the price of the order
   * @param remainingQuantity the quantity
   */
  public AddOrder(long orderId, Side side, long price, long remainingQuantity) {
    this.orderId = orderId;
    this.side = side;
    this.price = price;
    this.remainingQuantity = remainingQuantity;
  }

  public long orderId() {
    return orderId;
  }

  public Side side() {
    return side;
  }

  public long price() {
    return price;
  }

  public long quantity() {
    return remainingQuantity;
  }
}
