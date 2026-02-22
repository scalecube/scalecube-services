package io.scalecube.services.sut.typed;

import java.time.LocalDateTime;
import java.util.StringJoiner;

public class EndOfDayEvent extends TradeEvent {

  private LocalDateTime eodTime;

  public EndOfDayEvent() {}

  public EndOfDayEvent(long timestamp, long trackingNumber, long eventId, LocalDateTime eodTime) {
    super(timestamp, trackingNumber, eventId);
    this.eodTime = eodTime;
  }

  public LocalDateTime eodTime() {
    return eodTime;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", EndOfDayEvent.class.getSimpleName() + "[", "]")
        .add("eodTime=" + eodTime)
        .add("timestamp=" + timestamp)
        .add("trackingNumber=" + trackingNumber)
        .add("eventId=" + eventId)
        .toString();
  }
}
