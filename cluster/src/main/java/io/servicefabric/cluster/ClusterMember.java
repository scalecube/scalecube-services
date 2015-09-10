package io.servicefabric.cluster;

import static io.servicefabric.cluster.ClusterMemberStatus.SHUTDOWN;
import static io.servicefabric.cluster.ClusterMemberStatus.SUSPECTED;
import static io.servicefabric.cluster.ClusterMemberStatus.TRUSTED;

import io.servicefabric.transport.utils.KvPair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

/**
 * DTO class. Hosting cluster endpoint, status, metadata and update timestamp. Most important, contains --
 * {@link #compareTo(ClusterMember)} .
 */
public final class ClusterMember implements Comparable<ClusterMember> {
  private final ClusterEndpoint endpoint;
  private final ClusterMemberStatus status;
  private final List<KvPair<String, String>> metadata = new ArrayList<>();
  private final long lastUpdateTimestamp;

  ClusterMember(ClusterEndpoint endpoint, ClusterMemberStatus status, Map<String, String> metadata) {
    this(endpoint, status, metadata, System.currentTimeMillis());
  }

  ClusterMember(ClusterEndpoint endpoint, ClusterMemberStatus status, Map<String, String> metadata,
      long lastUpdateTimestamp) {
    this.endpoint = endpoint;
    this.status = status;
    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      this.metadata.add(new KvPair<>(entry.getKey(), entry.getValue()));
    }
    this.lastUpdateTimestamp = lastUpdateTimestamp;
  }

  public ClusterEndpoint endpoint() {
    return endpoint;
  }

  public ClusterMemberStatus status() {
    return status;
  }

  public Map<String, String> metadata() {
    Map<String, String> map = new HashMap<>();
    for (KvPair<String, String> pair : metadata) {
      map.put(pair.getKey(), pair.getValue());
    }
    return map;
  }

  public long lastUpdateTimestamp() {
    return lastUpdateTimestamp;
  }

  @Override
  public int compareTo(@Nonnull ClusterMember r1) {
    if (status == r1.status) {
      return 0;
    }
    if (status == SHUTDOWN) {
      return 1;
    }
    if (r1.status == SHUTDOWN) {
      return -1;
    }

    int clockCompare = Long.compare(lastUpdateTimestamp, r1.lastUpdateTimestamp);
    if (clockCompare < 0) {
      return -1;
    }
    if (clockCompare == 0 && (status == TRUSTED && r1.status == SUSPECTED)) {
      return -1;
    }

    return 1;
  }

  @Override
  public String toString() {
    return "ClusterMember{" + "endpoint=" + endpoint + ", status=" + status + ", metadata=" + metadata
        + ", lastUpdateTimestamp=" + lastUpdateTimestamp + '}';
  }
}
