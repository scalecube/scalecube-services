package io.scalecube.cluster.gossip;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.transport.Message;

import io.protostuff.Tag;

import java.util.Objects;

/**
 * Data model for gossip, include gossip id, qualifier and object need to disseminate.
 */
final class Gossip {
  /** The gossip id. */
  @Tag(1)
  private String gossipId;

  /** The gossip message. */
  @Tag(2)
  private Message message;

  public Gossip(String gossipId, Message message) {
    checkArgument(gossipId != null);
    checkArgument(message != null);
    this.gossipId = gossipId;
    this.message = message;
  }

  public String getGossipId() {
    return gossipId;
  }

  public Message getMessage() {
    return message;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    Gossip gossip = (Gossip) other;
    return Objects.equals(gossipId, gossip.gossipId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(gossipId);
  }

  @Override
  public String toString() {
    return "Gossip{" + "gossipId='" + gossipId + '\'' + ", message=" + message + '}';
  }
}
