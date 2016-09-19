package io.scalecube.cluster.gossip;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.cluster.Member;
import io.scalecube.transport.Message;

import java.util.Objects;

/**
 * Data model for gossip, include gossip id, qualifier and object need to disseminate.
 */
final class Gossip {

  private final String gossipId;
  private final Member origin;
  private final Message message;

  public Gossip(String gossipId, Member origin, Message message) {
    checkArgument(gossipId != null);
    checkArgument(origin != null);
    checkArgument(message != null);
    this.gossipId = gossipId;
    this.origin = origin;
    this.message = message;
  }

  public String gossipId() {
    return gossipId;
  }

  public Member origin() {
    return origin;
  }

  public Message message() {
    return message;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Gossip gossip = (Gossip) o;
    return Objects.equals(gossipId, gossip.gossipId) &&
        Objects.equals(origin, gossip.origin) &&
        Objects.equals(message, gossip.message);
  }

  @Override
  public int hashCode() {
    return Objects.hash(gossipId, origin, message);
  }

  @Override
  public String toString() {
    return "Gossip{gossipId=" + gossipId + ", origin=" + origin + ", message=" + message + '}';
  }
}
