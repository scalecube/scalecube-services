package io.scalecube.cluster.gossip;

import com.google.common.base.Preconditions;

/** Data related to gossip, maintained locally on each node. */
final class GossipState {

  /** Target gossip. */
  private final Gossip gossip;

  /** Local gossip period when gossip was received for the first time. */
  private final long infectionPeriod;

  GossipState(Gossip gossip, long infectionPeriod) {
    Preconditions.checkArgument(gossip != null);
    this.gossip = gossip;
    this.infectionPeriod = infectionPeriod;
  }

  public Gossip gossip() {
    return gossip;
  }

  public long infectionPeriod() {
    return infectionPeriod;
  }

  @Override
  public String toString() {
    return "GossipState{gossip=" + gossip + ", infectionPeriod=" + infectionPeriod + '}';
  }
}
