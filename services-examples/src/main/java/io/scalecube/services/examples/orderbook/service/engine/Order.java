package io.scalecube.services.examples.orderbook.service.engine;

public class Order {

  private PriceLevel level;

  private long id;

  private long remainingQuantity;

  /**
   * Create a new order for the price level.
   *
   * @param level the price level
   * @param id the order id
   * @param size the order size
   */
  public Order(PriceLevel level, long id, long size) {
    this.level = level;
    this.id = id;
    this.remainingQuantity = size;
  }

  public PriceLevel level() {
    return level;
  }

  public long id() {
    return id;
  }

  public long size() {
    return remainingQuantity;
  }

  public void reduce(long quantity) {
    remainingQuantity -= quantity;
  }

  public void resize(long size) {
    remainingQuantity = size;
  }
}
