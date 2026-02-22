package io.scalecube.services.sut.typed;

import java.math.BigDecimal;
import java.util.StringJoiner;

public class TradeExecutedEvent extends TradeEvent {

  private BigDecimal price;
  private BigDecimal quantity;
  private long tradeId;

  public TradeExecutedEvent() {}

  public TradeExecutedEvent(
      long timestamp,
      long trackingNumber,
      long eventId,
      BigDecimal price,
      BigDecimal quantity,
      long tradeId) {
    super(timestamp, trackingNumber, eventId);
    this.price = price;
    this.quantity = quantity;
    this.tradeId = tradeId;
  }

  public BigDecimal price() {
    return price;
  }

  public BigDecimal quantity() {
    return quantity;
  }

  public long tradeId() {
    return tradeId;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", TradeExecutedEvent.class.getSimpleName() + "[", "]")
        .add("price=" + price)
        .add("quantity=" + quantity)
        .add("tradeId=" + tradeId)
        .add("timestamp=" + timestamp)
        .add("trackingNumber=" + trackingNumber)
        .add("eventId=" + eventId)
        .toString();
  }
}
