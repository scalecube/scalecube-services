package io.scalecube.streams;

import io.scalecube.transport.Address;

import java.util.Optional;

public final class Event {

  enum Topic {
    //
    ReadSuccess,
    //
    ReadError,
    //
    Write,
    //
    WriteSuccess,
    //
    WriteError,
    //
    ChannelContextClosed,
    //
    ChannelContextSubscribed,
    //
    ChannelContextUnsubscribed
  }

  private final Topic topic; // not null
  private final Address address; // not null
  private final String identity; // not null
  private final Optional<StreamMessage> message; // nullable
  private final Optional<Throwable> error; // nullable

  private Event(Builder builder) {
    this.topic = builder.topic;
    this.address = builder.address;
    this.identity = builder.identity;
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

  public Optional<StreamMessage> getMessage() {
    return message;
  }

  public StreamMessage getMessageOrThrow() {
    return getMessage().get();
  }

  public Address getAddress() {
    return address;
  }

  public String getIdentity() {
    return identity;
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

  public boolean isWrite() {
    return topic == Topic.Write;
  }

  public boolean isWriteSuccess() {
    return topic == Topic.WriteSuccess;
  }

  public boolean isWriteError() {
    return topic == Topic.WriteError;
  }

  public boolean isChannelContextClosed() {
    return topic == Topic.ChannelContextClosed;
  }

  public boolean isChannelContextSubscribed() {
    return topic == Topic.ChannelContextSubscribed;
  }

  public boolean isChannelContextUnsubscribed() {
    return topic == Topic.ChannelContextUnsubscribed;
  }

  //// Builder

  public static class Builder {

    private final Topic topic; // not null
    private final Address address; // not null
    private final String identity; // not null
    private StreamMessage message; // nullable
    private Throwable error; // nullable

    public Builder(Topic topic, Address address, String identity) {
      this.topic = topic;
      this.address = address;
      this.identity = identity;
    }

    public Builder(Event other) {
      this(other.topic, other.address, other.identity);
      this.message = other.message.orElse(null);
      this.error = other.error.orElse(null);
    }

    public Builder message(StreamMessage message) {
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
