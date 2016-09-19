package io.scalecube.cluster.gossip;

import io.scalecube.cluster.Member;
import io.scalecube.transport.Address;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import java.util.HashSet;
import java.util.Set;

/** Data related to gossip, maintained locally on each node. */
final class GossipState {

  /** Target gossip. */
  private final Gossip gossip;

  /** Local time when gossip first period occur. */
  private final long period;

  /** How many times gossip was sent, increment before each send. */
  private int sent = 0;

  /** Set of members this gossip was received from or sent to. */
  private Set<Member> infected = new HashSet<>();

  GossipState(Gossip gossip, long period) {
    Preconditions.checkArgument(gossip != null);
    this.gossip = gossip;
    this.period = period;
  }

  public void addToInfected(Member member) {
    infected.add(member);
  }

  public boolean isInfected(Member member) {
    return infected.contains(member);
  }

  public void incrementSend() {
    sent++;
  }

  public Gossip gossip() {
    return gossip;
  }

  public int getSent() {
    return sent;
  }

  public long getPeriod() {
    return period;
  }

  @Override
  public String toString() {
    return "GossipState{gossip=" + gossip + ", sent=" + sent + ", period=" + period + ", members=" + infected + '}';
  }
}
