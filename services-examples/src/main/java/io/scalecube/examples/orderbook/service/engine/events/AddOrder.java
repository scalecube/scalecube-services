package io.scalecube.examples.orderbook.service.engine.events;

public class AddOrder {

  long orderId;
  Side side;
  long price;
  long remainingQuantity;

  public AddOrder() {}

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
