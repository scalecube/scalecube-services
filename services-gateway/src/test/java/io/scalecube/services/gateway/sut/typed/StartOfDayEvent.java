package io.scalecube.services.gateway.sut.typed;

import java.time.LocalDateTime;
import java.util.StringJoiner;

public class StartOfDayEvent {

  private long timestamp;
  private long trackingNumber;
  private long eventId;
  private LocalDateTime sodTime;

  public StartOfDayEvent() {}

  public StartOfDayEvent(long timestamp, long trackingNumber, long eventId, LocalDateTime sodTime) {
    this.timestamp = timestamp;
    this.trackingNumber = trackingNumber;
    this.eventId = eventId;
    this.sodTime = sodTime;
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

  public LocalDateTime sodTime() {
    return sodTime;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", StartOfDayEvent.class.getSimpleName() + "[", "]")
        .add("timestamp=" + timestamp)
        .add("trackingNumber=" + trackingNumber)
        .add("eventId=" + eventId)
        .add("sodTime=" + sodTime)
        .toString();
  }
}
