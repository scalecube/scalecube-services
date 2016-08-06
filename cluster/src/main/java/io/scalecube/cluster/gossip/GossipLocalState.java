package io.scalecube.cluster.gossip;

import io.scalecube.transport.Address;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import java.util.Set;

/** Data related to gossip, maintained locally on each node. */
final class GossipLocalState {
  /** Target gossip. */
  private Gossip gossip;
  /** How many times gossip was sent, increment before each send. */
  private int sent;
  /** Local time when gossip first period occur. */
  private long period;
  /** Set of members this gossip was received from. */
  private Set<Address> members;

  private GossipLocalState() {}

  public static GossipLocalState create(Gossip gossip, Address member, long period) {
    Preconditions.checkNotNull(gossip);
    GossipLocalState data = new GossipLocalState();
    data.gossip = gossip;
    data.members = Sets.newHashSet();
    if (member != null) {
      data.members.add(member);
    }
    data.period = period;
    data.sent = 0;
    return data;

  }

  public void addMember(Address source) {
    members.add(source);
  }

  public boolean containsMember(Address address) {
    return members.contains(address);
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
    return "GossipLocalState{" + "gossip=" + gossip + ", sent=" + sent + ", period=" + period + ", members=" + members
        + '}';
  }
}
