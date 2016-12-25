package io.scalecube.cluster.gossip;

import com.google.common.base.Preconditions;

import java.util.HashSet;
import java.util.Set;

/** Data related to gossip, maintained locally on each node. */
final class GossipState {

  /** Target gossip. */
  private final Gossip gossip;

  /** Local gossip period when gossip was received for the first time. */
  private final long infectionPeriod;

  /** Set of member IDs this gossip was received from. */
  private final Set<String> infected = new HashSet<>();

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

  public void addToInfected(String memberId) {
    infected.add(memberId);
  }

  public boolean isInfected(String memberId) {
    return infected.contains(memberId);
  }

  @Override
  public String toString() {
    return "GossipState{gossip=" + gossip
        + ", infectionPeriod=" + infectionPeriod
        + ", infected=" + infected
        + '}';
  }
}
