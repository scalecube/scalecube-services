package io.scalecube.cluster.membership;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.cluster.Member;
import io.scalecube.transport.Address;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * Cluster membership record which represents member, status, and incarnation.
 */
@Immutable
final class MembershipRecord implements Comparable<MembershipRecord> {

  private final Member member;
  private final MemberStatus status;
  private final long incarnation;

  /**
   * Instantiates new instance of membership record with given member, status and incarnation.
   */
  public MembershipRecord(Member member, MemberStatus status, long incarnation) {
    checkArgument(member != null);
    checkArgument(status != null);
    this.member = member;
    this.status = status;
    this.incarnation = incarnation;
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

  public long incarnation() {
    return incarnation;
  }

  @Override
  public int compareTo(@Nonnull MembershipRecord r1) {
    if (status == r1.status) {
      return 0;
    }
    if (status == MemberStatus.DEAD) {
      return 1;
    }
    if (r1.status == MemberStatus.DEAD) {
      return -1;
    }

    int clockCompare = Long.compare(incarnation, r1.incarnation);
    if (clockCompare < 0) {
      return -1;
    }
    if (clockCompare == 0 && (status == MemberStatus.ALIVE && r1.status == MemberStatus.SUSPECT)) {
      return -1;
    }

    return 1;
  }

  @Override
  public String toString() {
    return "MembershipRecord{member=" + member
        + ", status=" + status
        + ", incarnation=" + incarnation
        + '}';
  }
}
