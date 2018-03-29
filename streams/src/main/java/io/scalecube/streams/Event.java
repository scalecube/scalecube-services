package io.scalecube.streams;

import io.scalecube.transport.Address;

import java.util.Objects;
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

  public static Builder readSuccess(Address address) {
    return new Event.Builder(Topic.ReadSuccess).address(address);
  }

  public static Builder readError(Address address) {
    return new Event.Builder(Topic.ReadError).address(address);
  }

  public static Builder write(Address address) {
    return new Event.Builder(Topic.Write).address(address);
  }

  public static Builder writeSuccess(Address address) {
    return new Event.Builder(Topic.WriteSuccess).address(address);
  }

  public static Builder writeError(Address address) {
    return new Event.Builder(Topic.WriteError).address(address);
  }

  public static Builder channelContextClosed(Address address) {
    return new Event.Builder(Topic.ChannelContextClosed).address(address);
  }

  public static Builder channelContextSubscribed(Address address) {
    return new Event.Builder(Topic.ChannelContextSubscribed).address(address);
  }

  public static Builder channelContextUnsubscribed(Address address) {
    return new Event.Builder(Topic.ChannelContextUnsubscribed).address(address);
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

  @Override
  public String toString() {
    return "Event [topic=" + topic
        + ", address=" + address
        + ", identity=" + identity
        + ", message=" + message.orElse(null)
        + ", error=" + error.orElse(null)
        + "]";
  }

  //// Builder

  public static class Builder {

    private Topic topic; // not null
    private Address address; // not null
    private String identity; // not null
    private StreamMessage message; // nullable
    private Throwable error; // nullable

    private Builder(Topic topic) {
      this.topic = topic;
    }

    private Builder(Event other) {
      this(other.topic);
      this.address = other.address;
      this.identity = other.identity;
      this.message = other.message.orElse(null);
      this.error = other.error.orElse(null);
    }

    public Builder address(Address address) {
      this.address = address;
      return this;
    }

    public Builder identity(String identity) {
      this.identity = identity;
      return this;
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
      Objects.requireNonNull(topic);
      Objects.requireNonNull(address);
      Objects.requireNonNull(identity);
      return new Event(this);
    }
  }
}
