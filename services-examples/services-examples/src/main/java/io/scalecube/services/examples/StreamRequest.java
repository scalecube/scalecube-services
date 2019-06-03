package io.scalecube.services.examples;

public class StreamRequest {

  private long intervalMillis;
  private int messagesPerInterval;

  public long getIntervalMillis() {
    return intervalMillis;
  }

  public StreamRequest setIntervalMillis(long intervalMillis) {
    this.intervalMillis = intervalMillis;
    return this;
  }

  public int getMessagesPerInterval() {
    return messagesPerInterval;
  }

  public StreamRequest setMessagesPerInterval(int messagesPerInterval) {
    this.messagesPerInterval = messagesPerInterval;
    return this;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("StreamRequest{");
    sb.append("intervalMillis=").append(intervalMillis);
    sb.append(", messagesPerInterval=").append(messagesPerInterval);
    sb.append('}');
    return sb.toString();
  }
}
