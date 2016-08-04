package io.scalecube.cluster;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.transport.TransportEndpoint;

import java.util.Collections;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * DTO class. Hosting cluster endpoint, status, metadata and update timestamp. Most important, contains --
 * {@link #compareTo(ClusterMember)} .
 */
@Immutable
public final class ClusterMember implements Comparable<ClusterMember> {
  private final String id;
  private final TransportEndpoint endpoint;
  private final Map<String, String> metadata;
  private final ClusterMemberStatus status;
  private final long lastUpdateTimestamp;

  ClusterMember(TransportEndpoint endpoint, ClusterMemberStatus status, Map<String, String> metadata) {
    this(endpoint, status, metadata, System.currentTimeMillis());
  }

  ClusterMember(TransportEndpoint endpoint, ClusterMemberStatus status, Map<String, String> metadata,
      long lastUpdateTimestamp) {
    checkArgument(endpoint != null);
    checkArgument(status != null);
    this.id = endpoint.id();
    this.endpoint = endpoint;
    this.status = status;
    this.metadata = metadata;
    this.lastUpdateTimestamp = lastUpdateTimestamp;
  }

  @Nonnull
  public String id() {
    return id;
  }

  @Nonnull
  public TransportEndpoint endpoint() {
    return endpoint;
  }

  @Nonnull
  public ClusterMemberStatus status() {
    return status;
  }

  public Map<String, String> metadata() {
    return Collections.unmodifiableMap(metadata);
  }

  public long lastUpdateTimestamp() {
    return lastUpdateTimestamp;
  }

  @Override
  public int compareTo(@Nonnull ClusterMember r1) {
    if (status == r1.status) {
      return 0;
    }
    if (status == ClusterMemberStatus.SHUTDOWN) {
      return 1;
    }
    if (r1.status == ClusterMemberStatus.SHUTDOWN) {
      return -1;
    }

    int clockCompare = Long.compare(lastUpdateTimestamp, r1.lastUpdateTimestamp);
    if (clockCompare < 0) {
      return -1;
    }
    if (clockCompare == 0 && (status == ClusterMemberStatus.TRUSTED && r1.status == ClusterMemberStatus.SUSPECTED)) {
      return -1;
    }

    return 1;
  }

  @Override
  public String toString() {
    return "ClusterMember{endpoint=" + endpoint
        + ", status=" + status
        + ", metadata=" + metadata
        + ", lastUpdateTimestamp=" + lastUpdateTimestamp
        + '}';
  }
}
