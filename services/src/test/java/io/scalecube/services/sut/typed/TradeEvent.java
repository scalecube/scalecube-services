package io.scalecube.services.sut.typed;

import java.math.BigDecimal;
import java.util.StringJoiner;

public class TradeEvent {

  private long timestamp;
  private long trackingNumber;
  private long eventId;
  private BigDecimal price;
  private BigDecimal quantity;
  private long tradeId;

  public TradeEvent() {}

  public TradeEvent(
      long timestamp,
      long trackingNumber,
      long eventId,
      BigDecimal price,
      BigDecimal quantity,
      long tradeId) {
    this.timestamp = timestamp;
    this.trackingNumber = trackingNumber;
    this.eventId = eventId;
    this.price = price;
    this.quantity = quantity;
    this.tradeId = tradeId;
  }

  public long timestamp() {
    return timestamp;
  }

  public long trackingNumber() {
    return trackingNumber;
  }

  public long eventId() {
    return eventId;
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
    return new StringJoiner(", ", TradeEvent.class.getSimpleName() + "[", "]")
        .add("timestamp=" + timestamp)
        .add("trackingNumber=" + trackingNumber)
        .add("eventId=" + eventId)
        .add("price=" + price)
        .add("quantity=" + quantity)
        .add("tradeId=" + tradeId)
        .toString();
  }
}
