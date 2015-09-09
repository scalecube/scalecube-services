package io.servicefabric.cluster;

import static com.google.common.collect.Maps.filterValues;
import static com.google.common.collect.Maps.newHashMap;
import static io.servicefabric.cluster.ClusterMemberStatus.REMOVED;
import static io.servicefabric.cluster.ClusterMemberStatus.SUSPECTED;
import static io.servicefabric.cluster.ClusterMemberStatus.TRUSTED;

import io.servicefabric.cluster.fdetector.FailureDetectorEvent;

import com.google.common.base.Predicate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

final class ClusterMembershipTable {
  private final ConcurrentMap<ClusterEndpoint, ClusterMember> membership = new ConcurrentHashMap<>();

  List<ClusterMember> merge(ClusterMembershipData data) {
    List<ClusterMember> updates = new ArrayList<>();
    for (ClusterMember record : data.getMembership()) {
      updates.addAll(merge(record));
    }
    return updates;
  }

  List<ClusterMember> merge(ClusterMember r1) {
    List<ClusterMember> updates = new ArrayList<>(1);
    ClusterMember r0 = membership.putIfAbsent(r1.endpoint(), r1);
    if (r0 == null) {
      updates.add(r1);
    } else if (r0.compareTo(r1) < 0) {
      if (membership.replace(r1.endpoint(), r0, r1)) {
        updates.add(r1);
      } else {
        return merge(r1);
      }
    }
    return updates;
  }

  ClusterMember get(ClusterEndpoint member) {
    return membership.get(member);
  }

  List<ClusterMember> remove(ClusterEndpoint member) {
    List<ClusterMember> updates = new ArrayList<>(1);
    ClusterMember r0 = membership.remove(member);
    if (r0 != null) {
      updates.add(new ClusterMember(member, REMOVED, r0.metadata()));
    }
    return updates;
  }

  List<ClusterMember> merge(FailureDetectorEvent event) {
    ClusterMember r0 = membership.get(event.endpoint());
    if (r0 != null) {
      return merge(new ClusterMember(event.endpoint(), event.status(), r0.metadata()));
    } else {
      return Collections.emptyList();
    }
  }

  List<ClusterMember> asList() {
    return new ArrayList<>(membership.values());
  }

  /** Getting {@code TRUSTED} or {@code SUSPECTED} snapshot of {@link #membership}. */
  Map<ClusterEndpoint, ClusterMember> getTrustedOrSuspected() {
    return newHashMap(filterValues(membership, new Predicate<ClusterMember>() {
      @Override
      public boolean apply(ClusterMember input) {
        return input.status() == TRUSTED || input.status() == SUSPECTED;
      }
    }));
  }
}
