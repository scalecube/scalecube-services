package io.scalecube.cluster.membership;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.cluster.Member;
import io.scalecube.transport.Address;

import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * Cluster membership record which represents member, status, and timestamp. Most important, contains -
 * {@link #compareTo(MembershipRecord)} .
 */
@Immutable
public final class MembershipRecord implements Comparable<MembershipRecord> {
  private final Member member;
  private final MemberStatus status;
  private final long timestamp;

  /**
   * Instantiates new instance of membership record with given member, status and current timestamp.
   */
  public MembershipRecord(Member member, MemberStatus status) {
    this(member, status, System.currentTimeMillis());
  }

  /**
   * Instantiates new instance of membership record with given member, status and timestamp.
   */
  public MembershipRecord(Member member, MemberStatus status, long timestamp) {
    checkArgument(member != null);
    checkArgument(status != null);
    this.member = member;
    this.status = status;
    this.timestamp = timestamp;
  }

  @Nonnull
  public Member member() {
    return member;
  }

  @Nonnull
  public String id() {
    return member.id();
  }

  @Nonnull
  public Address address() {
    return member.address();
  }

  @Nonnull
  public MemberStatus status() {
    return status;
  }

  public Map<String, String> metadata() {
    return member.metadata();
  }

  public long timestamp() {
    return timestamp;
  }

  @Override
  public int compareTo(@Nonnull MembershipRecord r1) {
    if (status == r1.status) {
      return 0;
    }
    if (status == MemberStatus.SHUTDOWN) {
      return 1;
    }
    if (r1.status == MemberStatus.SHUTDOWN) {
      return -1;
    }

    int clockCompare = Long.compare(timestamp, r1.timestamp);
    if (clockCompare < 0) {
      return -1;
    }
    if (clockCompare == 0 && (status == MemberStatus.TRUSTED && r1.status == MemberStatus.SUSPECTED)) {
      return -1;
    }

    return 1;
  }

  @Override
  public String toString() {
    return "ClusterMember{member=" + member
        + ", status=" + status
        + ", timestamp=" + timestamp
        + '}';
  }
}
