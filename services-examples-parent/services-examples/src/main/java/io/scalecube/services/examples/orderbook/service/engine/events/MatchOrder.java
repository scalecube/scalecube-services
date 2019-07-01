package io.scalecube.services.examples.orderbook.service.engine.events;

public class MatchOrder {
  long restingOrderId;
  long incomingOrderId;
  Side incomingSide;
  long price;
  long executedQuantity;
  long remainingQuantity;

  public MatchOrder() {}

  /**
   * Create a new Match Order.
   *
   * @param restingOrderId the id of the resting order
   * @param incomingOrderId the id of the incoming order
   * @param incomingSide the side of the incoming id (implies the side of the resting... )
   * @param price the price
   * @param executedQuantity the quantity matched.
   * @param remainingQuantity the remaining quantity of the incoming order
   */
  public MatchOrder(
      long restingOrderId,
      long incomingOrderId,
      Side incomingSide,
      long price,
      long executedQuantity,
      long remainingQuantity) {

    this.restingOrderId = restingOrderId;
    this.incomingOrderId = incomingOrderId;
    this.incomingSide = incomingSide;
    this.price = price;
    this.executedQuantity = executedQuantity;
    this.remainingQuantity = remainingQuantity;
  }

  public long restingOrderId() {
    return restingOrderId;
  }

  public long incomingOrderId() {
    return incomingOrderId;
  }

  public Side incomingSide() {
    return incomingSide;
  }

  public long price() {
    return price;
  }

  public long executedQuantity() {
    return executedQuantity;
  }

  public long remainingQuantity() {
    return remainingQuantity;
  }

  @Override
  public String toString() {
    return "Match [restingOrderId="
        + restingOrderId
        + ", incomingOrderId="
        + incomingOrderId
        + ", incomingSide="
        + incomingSide
        + ", price="
        + price
        + ", executedQuantity="
        + executedQuantity
        + ", remainingQuantity="
        + remainingQuantity
        + "]";
  }
}
