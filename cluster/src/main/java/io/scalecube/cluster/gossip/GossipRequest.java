package io.scalecube.cluster.gossip;

import io.protostuff.Tag;

import java.util.ArrayList;
import java.util.List;

/**
 * Gossip request which be transmitted through the network, contains list of gossips.
 */
final class GossipRequest {
  @Tag(1)
  private final List<Gossip> gossips;

  @Tag(2)
  private final String from;

  public GossipRequest(List<Gossip> gossips, String from) {
    this.gossips = new ArrayList<>(gossips);
    this.from = from;
  }

  public List<Gossip> gossips() {
    return gossips;
  }

  public String from() {
    return from;
  }

  @Override
  public String toString() {
    return "GossipRequest{gossips=" + gossips + ", from=" + from + '}';
  }
}
