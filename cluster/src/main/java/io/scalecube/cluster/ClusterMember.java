package io.scalecube.cluster;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.transport.Address;

import java.util.Collections;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * DTO class. Hosting cluster member id, address, status, metadata and update timestamp. Most important, contains --
 * {@link #compareTo(ClusterMember)} .
 */
@Immutable
public final class ClusterMember implements Comparable<ClusterMember> {
  private final String id;
  private final Address address;
  private final Map<String, String> metadata;
  private final ClusterMemberStatus status;
  private final long timestamp;

  /**
   * Instantiates new instance of cluster member with given parameters and current timestamp.
   */
  public ClusterMember(String id, Address address, ClusterMemberStatus status, Map<String, String> metadata) {
    this(id, address, status, metadata, System.currentTimeMillis());
  }

  /**
   * Instantiates new instance of cluster member with given parameters.
   */
  public ClusterMember(String id, Address address, ClusterMemberStatus status, Map<String, String> metadata,
                long timestamp) {
    checkArgument(id != null);
    checkArgument(address != null);
    checkArgument(status != null);
    this.id = id;
    this.address = address;
    this.status = status;
    this.metadata = metadata;
    this.timestamp = timestamp;
  }

  @Nonnull
  public String id() {
    return id;
  }

  @Nonnull
  public Address address() {
    return address;
  }

  @Nonnull
  public ClusterMemberStatus status() {
    return status;
  }

  public Map<String, String> metadata() {
    return Collections.unmodifiableMap(metadata);
  }

  public long timestamp() {
    return timestamp;
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

    int clockCompare = Long.compare(timestamp, r1.timestamp);
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
    return "ClusterMember{id=" + id
        + ", address=" + address
        + ", status=" + status
        + ", metadata=" + metadata
        + ", lastUpdateTimestamp=" + timestamp
        + '}';
  }
}
