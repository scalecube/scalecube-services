package io.scalecube.cluster.gossip;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.transport.Message;

import java.util.Objects;

/**
 * Data model for gossip, include gossip id, qualifier and object need to disseminate.
 */
final class Gossip {

  private final String gossipId;
  private final Message message;

  public Gossip(String gossipId, Message message) {
    checkArgument(gossipId != null);
    checkArgument(message != null);
    this.gossipId = gossipId;
    this.message = message;
  }

  public String gossipId() {
    return gossipId;
  }

  public Message message() {
    return message;
  }

  @Override
  public boolean equals(Object that) {
    if (this == that) {
      return true;
    }
    if (that == null || getClass() != that.getClass()) {
      return false;
    }
    Gossip gossip = (Gossip) that;
    return Objects.equals(gossipId, gossip.gossipId)
        && Objects.equals(message, gossip.message);
  }

  @Override
  public int hashCode() {
    return Objects.hash(gossipId, message);
  }

  @Override
  public String toString() {
    return "Gossip{gossipId=" + gossipId + ", message=" + message + '}';
  }
}
