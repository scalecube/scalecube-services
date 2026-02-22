package io.scalecube.services.sut.typed;

public abstract class TradeEvent {

  protected long timestamp;
  protected long trackingNumber;
  protected long eventId;

  public TradeEvent() {}

  public TradeEvent(long timestamp, long trackingNumber, long eventId) {
    this.timestamp = timestamp;
    this.trackingNumber = trackingNumber;
    this.eventId = eventId;
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
}
