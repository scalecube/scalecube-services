package io.servicefabric.cluster;

import io.protostuff.Tag;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** DTO class. Contains a snapshot of cluster members and cluster identifier. */
final class ClusterMembershipData {
  /** A 'snapshot' of cluster members. */
  @Tag(1)
  private List<ClusterMember> membership = new ArrayList<>();
  /** Sort of cluster identifier. Only makes sense at cluster membership SYNC/SYNC_ACK transitions. */
  @Tag(2)
  private String syncGroup;

  ClusterMembershipData(Collection<ClusterMember> membership, String syncGroup) {
    this.membership = new ArrayList<>(membership);
    this.syncGroup = syncGroup;
  }

  Collection<ClusterMember> getMembership() {
    return new ArrayList<>(membership);
  }

  String getSyncGroup() {
    return syncGroup;
  }

  @Override
  public String toString() {
    return "ClusterMembershipData{" + "membership=" + membership + ", syncGroup='" + syncGroup + '\'' + '}';
  }
}
