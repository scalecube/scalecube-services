package io.scalecube.cluster;

import static io.scalecube.cluster.ClusterMemberStatus.REMOVED;
import static io.scalecube.cluster.ClusterMemberStatus.SUSPECTED;
import static io.scalecube.cluster.ClusterMemberStatus.TRUSTED;

import io.scalecube.cluster.fdetector.FailureDetectorEvent;
import io.scalecube.transport.Address;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

final class ClusterMembershipTable {

  private final ConcurrentMap<String, ClusterMember> membership = new ConcurrentHashMap<>();

  public List<ClusterMember> merge(ClusterMembershipData data) {
    List<ClusterMember> updates = new ArrayList<>();
    for (ClusterMember record : data.getMembership()) {
      updates.addAll(merge(record));
    }
    return updates;
  }

  public List<ClusterMember> merge(ClusterMember r1) {
    List<ClusterMember> updates = new ArrayList<>(1);
    ClusterMember r0 = membership.putIfAbsent(r1.id(), r1);
    if (r0 == null) {
      updates.add(r1);
    } else if (r0.compareTo(r1) < 0) {
      if (membership.replace(r1.id(), r0, r1)) {
        updates.add(r1);
      } else {
        return merge(r1);
      }
    }
    return updates;
  }

  public List<ClusterMember> merge(FailureDetectorEvent event) {
    ClusterMember r0 = get(event.address());
    if (r0 != null) {
      return merge(new ClusterMember(r0.id(), r0.address(), event.status(), r0.metadata()));
    } else {
      return Collections.emptyList();
    }
  }

  public ClusterMember get(Address address) {
    // TODO [AK]: Temporary solution, should be optimized!!!
    for (ClusterMember member : membership.values()) {
      if (member.address().equals(address)) {
        return member;
      }
    }
    return null;
  }

  public ClusterMember get(String id) {
    return membership.get(id);
  }

  public List<ClusterMember> remove(String id) {
    List<ClusterMember> updates = new ArrayList<>(1);
    ClusterMember r0 = membership.remove(id);
    if (r0 != null) {
      updates.add(new ClusterMember(r0.id(), r0.address(), REMOVED, r0.metadata()));
    }
    return updates;
  }

  public List<ClusterMember> asList() {
    return new ArrayList<>(membership.values());
  }

  /**
   * Getting {@code TRUSTED} or {@code SUSPECTED} member's addresses.
   */
  public Collection<Address> getTrustedOrSuspectedMembers() {
    Collection<Address> addresses = new ArrayList<>();
    for (ClusterMember member : membership.values()) {
      if (member.status() == TRUSTED || member.status() == SUSPECTED) {
        addresses.add(member.address());
      }
    }
    return addresses;
  }
}
