package io.scalecube.ipc;

import io.scalecube.transport.Address;

import java.util.Optional;

public final class Event {

  enum Topic {
    ReadSuccess, ReadError, MessageWrite, WriteSuccess, WriteError, ChannelContextInactive
  }

  private final Topic topic; // not null
  private final ChannelContext channelContext; // not null
  private final Optional<ServiceMessage> message; // nullable
  private final Optional<Throwable> error; // nullable

  private Event(Builder builder) {
    this.topic = builder.topic;
    this.channelContext = builder.channelContext;
    this.message = Optional.ofNullable(builder.message);
    this.error = Optional.ofNullable(builder.error);
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

  public ServiceMessage getMessageOrThrow() {
    return getMessage().get();
  }

  public Address getAddress() {
    return channelContext.getAddress();
  }

  public String getIdentity() {
    return channelContext.getId();
  }

  public Optional<Throwable> getError() {
    return error;
  }

  public boolean hasError() {
    return getError().isPresent();
  }

  public Throwable getErrorOrThrow() {
    return getError().get();
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

  public boolean isChannelContextInactive() {
    return topic == Topic.ChannelContextInactive;
  }

  //// Builder

  public static class Builder {

    private final Topic topic; // not null
    private final ChannelContext channelContext; // not null
    private ServiceMessage message; // nullable
    private Throwable error; // nullable

    public Builder(Topic topic, ChannelContext channelContext) {
      this.topic = topic;
      this.channelContext = channelContext;
    }

    public Builder(Event other) {
      this.topic = other.topic;
      this.channelContext = other.channelContext;
      this.message = other.message.orElse(null);
      this.error = other.error.orElse(null);
    }

    public Builder message(ServiceMessage message) {
      this.message = message;
      return this;
    }

    public Builder error(Throwable throwable) {
      this.error = throwable;
      return this;
    }

    public Event build() {
      return new Event(this);
    }
  }
}
