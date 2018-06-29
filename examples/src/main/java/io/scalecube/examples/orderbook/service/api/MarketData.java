package io.scalecube.examples.orderbook.service.api;

public class MarketData {

  private Integer price;
  private Integer amount;
  private String type;

  public MarketData(String type, Integer price, Integer amount) {
    this.price = price;
    this.amount = amount;
    this.type = type;
  }

  public Integer price() {
    return this.price;
  }

  public Integer amount() {
    return this.amount;
  }

  public String type() {
    return this.type;
  }

  @Override
  public String toString() {
    return "MarketData [price=" + price + ", amount=" + amount + ", type=" + type + "]";
  }
}
