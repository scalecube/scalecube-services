package io.scalecube.cluster.gossip;

import io.scalecube.cluster.Member;

import com.google.common.base.Preconditions;

import java.util.HashSet;
import java.util.Set;

/** Data related to gossip, maintained locally on each node. */
final class GossipState {

  /** Target gossip. */
  private final Gossip gossip;

  /** Local gossip period when gossip was received for the first time. */
  private final long infectionPeriod;

  /** Set of members this gossip was received from. */
  private final Set<Member> infected = new HashSet<>();

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

  public void addToInfected(Member member) {
    infected.add(member);
  }

  public boolean isInfected(Member member) {
    return infected.contains(member);
  }

  @Override
  public String toString() {
    return "GossipState{gossip=" + gossip
        + ", infectionPeriod=" + infectionPeriod
        + ", infected=" + infected
        + '}';
  }
}
