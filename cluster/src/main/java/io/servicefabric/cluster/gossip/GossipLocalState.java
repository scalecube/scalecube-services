package io.servicefabric.cluster.gossip;

import io.servicefabric.transport.TransportEndpoint;

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
  /** Set of endpoints this gossip was received from. */
  private Set<TransportEndpoint> members;

  private GossipLocalState() {}

  public static GossipLocalState create(Gossip gossip, TransportEndpoint member, long period) {
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

  public void addMember(TransportEndpoint source) {
    members.add(source);
  }

  public boolean containsMember(TransportEndpoint endpoint) {
    return members.contains(endpoint);
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
