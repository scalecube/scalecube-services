package io.scalecube.services.sut.typed;

import java.time.LocalDateTime;
import java.util.StringJoiner;

public class StartOfDayEvent extends TradeEvent {

  private LocalDateTime sodTime;

  public StartOfDayEvent() {}

  public StartOfDayEvent(long timestamp, long trackingNumber, long eventId, LocalDateTime sodTime) {
    super(timestamp, trackingNumber, eventId);
    this.sodTime = sodTime;
  }

  public LocalDateTime sodTime() {
    return sodTime;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", StartOfDayEvent.class.getSimpleName() + "[", "]")
        .add("sodTime=" + sodTime)
        .add("timestamp=" + timestamp)
        .add("trackingNumber=" + trackingNumber)
        .add("eventId=" + eventId)
        .toString();
  }
}
