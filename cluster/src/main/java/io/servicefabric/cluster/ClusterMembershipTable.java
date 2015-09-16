package io.servicefabric.cluster;

import static io.servicefabric.cluster.ClusterMemberStatus.REMOVED;
import static io.servicefabric.cluster.ClusterMemberStatus.SUSPECTED;
import static io.servicefabric.cluster.ClusterMemberStatus.TRUSTED;

import io.servicefabric.cluster.fdetector.FailureDetectorEvent;
import io.servicefabric.transport.TransportEndpoint;

import com.google.common.base.Predicate;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

final class ClusterMembershipTable {

  private static final Predicate<ClusterMember> TRUSTED_OR_SUSPECTED_PREDICATE = new Predicate<ClusterMember>() {
    @Override
    public boolean apply(ClusterMember input) {
      return input.status() == TRUSTED || input.status() == SUSPECTED;
    }
  };

  private static final Maps.EntryTransformer<String, ClusterMember, TransportEndpoint> MEMBER_TO_ENDPOINT_TRANSFORMER =
      new Maps.EntryTransformer<String, ClusterMember, TransportEndpoint>() {
        @Override
        public TransportEndpoint transformEntry(String key, ClusterMember value) {
          return value.endpoint();
        }
      };


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
    ClusterMember r0 = membership.get(event.endpoint().id());
    if (r0 != null) {
      return merge(new ClusterMember(event.endpoint(), event.status(), r0.metadata()));
    } else {
      return Collections.emptyList();
    }
  }

  public ClusterMember get(TransportEndpoint endpoint) {
    return membership.get(endpoint.id());
  }

  public ClusterMember get(String id) {
    return membership.get(id);
  }

  public List<ClusterMember> remove(TransportEndpoint endpoint) {
    List<ClusterMember> updates = new ArrayList<>(1);
    ClusterMember r0 = membership.remove(endpoint.id());
    if (r0 != null) {
      updates.add(new ClusterMember(endpoint, REMOVED, r0.metadata()));
    }
    return updates;
  }


  public List<ClusterMember> asList() {
    return new ArrayList<>(membership.values());
  }

  /**
   * Getting {@code TRUSTED} or {@code SUSPECTED} member's endpoints.
   */
  public Collection<TransportEndpoint> getTrustedOrSuspectedEndpoints() {
    Map<String, ClusterMember> suspectedOrTrusted = Maps.filterValues(membership, TRUSTED_OR_SUSPECTED_PREDICATE);
    return Maps.transformEntries(suspectedOrTrusted, MEMBER_TO_ENDPOINT_TRANSFORMER).values();
  }
}
