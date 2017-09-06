package io.scalecube.examples.services.stocks;

public class Quote {



  private String ticker;
  private String exchange;
  private long lastTradeTime;

  private double price;
  private double change;

  private Quote(String excahnge, String ticker, Float price) {
    this(excahnge, ticker, price, 0F);
  }

  private Quote(String excahnge, String ticker, double price, double change) {
    this.exchange = excahnge;
    this.ticker = ticker;
    this.price = price;
    this.lastTradeTime = System.currentTimeMillis();
    this.change = change;
  }

  public String ticker() {
    return this.ticker;
  }

  public String exchange() {
    return this.exchange;
  }

  public long lastTradeTime() {
    return this.lastTradeTime;
  }

  public double price() {
    return this.price;
  }

  public double change() {
    return this.change;
  }

  public static Quote create(String exchange, String ticker, Float price) {
    return new Quote(exchange, ticker, price);
  }

  public Quote update(double change) {
    price = price * change;
    return new Quote(this.exchange, this.ticker, price, change);
  }

  @Override
  public String toString() {
    return "Quote [ticker=" + ticker + ", exchange=" + exchange + ", lastTradeTime=" + lastTradeTime + ", price="
        + price + ", change=" + change + "]";
  }
}
