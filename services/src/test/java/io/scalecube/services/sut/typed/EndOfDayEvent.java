package io.scalecube.services.sut.typed;

import java.time.LocalDateTime;
import java.util.StringJoiner;

public class EndOfDayEvent {

  private long timestamp;
  private long trackingNumber;
  private long eventId;
  private LocalDateTime eodTime;

  public EndOfDayEvent() {}

  public EndOfDayEvent(long timestamp, long trackingNumber, long eventId, LocalDateTime eodTime) {
    this.timestamp = timestamp;
    this.trackingNumber = trackingNumber;
    this.eventId = eventId;
    this.eodTime = eodTime;
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

  public LocalDateTime eodTime() {
    return eodTime;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", EndOfDayEvent.class.getSimpleName() + "[", "]")
        .add("timestamp=" + timestamp)
        .add("trackingNumber=" + trackingNumber)
        .add("eventId=" + eventId)
        .add("eodTime=" + eodTime)
        .toString();
  }
}
