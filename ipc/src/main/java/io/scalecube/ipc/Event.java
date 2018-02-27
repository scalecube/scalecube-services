package io.scalecube.ipc;

import io.scalecube.transport.Address;

import java.util.Optional;

public final class Event {

  enum Topic {
    ReadSuccess, ReadError, MessageWrite, WriteSuccess, WriteError
  }

  private final Topic topic; // not null
  private final ChannelContext channelContext; // not null
  private final Optional<ServiceMessage> message; // nullable
  private final Optional<Throwable> errorCause; // nullable

  private Event(Builder builder) {
    this.topic = builder.topic;
    this.channelContext = builder.channelContext;
    this.message = Optional.ofNullable(builder.message);
    this.errorCause = Optional.ofNullable(builder.errorCause);
  }

  public static Builder copyFrom(Event other) {
    return new Builder(other);
  }

  //// Getters

  public Topic getTopic() {
    return topic;
  }

  public Optional<ServiceMessage> getMessage() {
    return message;
  }

  public Address getAddress() {
    return channelContext.getAddress();
  }

  public String getIdentity() {
    return channelContext.getId();
  }

  public Optional<Throwable> getErrorCause() {
    return errorCause;
  }

  public boolean isReadSuccess() {
    return topic == Topic.ReadSuccess;
  }

  public boolean isReadError() {
    return topic == Topic.ReadError;
  }

  public boolean isMessageWrite() {
    return topic == Topic.MessageWrite;
  }

  public boolean isWriteSuccess() {
    return topic == Topic.WriteSuccess;
  }

  public boolean isWriteError() {
    return topic == Topic.WriteError;
  }

  //// Builder

  public static class Builder {

    private final Topic topic; // not null
    private final ChannelContext channelContext; // not null
    private ServiceMessage message; // nullable
    private Throwable errorCause; // nullable

    public Builder(Topic topic, ChannelContext channelContext) {
      this.topic = topic;
      this.channelContext = channelContext;
    }

    public Builder(Event other) {
      this.topic = other.topic;
      this.channelContext = other.channelContext;
      this.message = other.message.orElse(null);
      this.errorCause = other.errorCause.orElse(null);
    }

    public Builder message(ServiceMessage message) {
      this.message = message;
      return this;
    }

    public Builder error(Throwable throwable) {
      this.errorCause = throwable;
      return this;
    }

    public Event build() {
      return new Event(this);
    }
  }
}
