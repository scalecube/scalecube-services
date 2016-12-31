package io.scalecube.cluster.membership;

import static com.google.common.base.Preconditions.checkArgument;
import static io.scalecube.cluster.membership.MemberStatus.ALIVE;
import static io.scalecube.cluster.membership.MemberStatus.DEAD;
import static io.scalecube.cluster.membership.MemberStatus.SUSPECT;

import io.scalecube.cluster.Member;
import io.scalecube.transport.Address;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * Cluster membership record which represents member, status, and incarnation.
 */
@Immutable
final class MembershipRecord {

  private final Member member;
  private final MemberStatus status;
  private final int incarnation;

  /**
   * Instantiates new instance of membership record with given member, status and incarnation.
   */
  public MembershipRecord(Member member, MemberStatus status, int incarnation) {
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

  public boolean isAlive() {
    return status == ALIVE;
  }

  public boolean isSuspect() {
    return status == SUSPECT;
  }

  public boolean isDead() {
    return status == DEAD;
  }

  public int incarnation() {
    return incarnation;
  }

  /**
   * Checks either this record overrides given record.
   *
   * @param r0 existing record in membership table
   * @return true if this record overrides exiting; false otherwise
   */
  public boolean isOverrides(MembershipRecord r0) {
    if (r0 == null) {
      return isAlive();
    }
    checkArgument(this.member.id().equals(r0.member.id()), "Can't compare records for different members");
    if (r0.status == DEAD) {
      return false;
    }
    if (status == DEAD) {
      return true;
    }
    if (incarnation == r0.incarnation) {
      return (status != r0.status) && (status == SUSPECT);
    } else {
      return incarnation > r0.incarnation;
    }
  }

  @Override
  public boolean equals(Object that) {
    if (this == that) {
      return true;
    }
    if (that == null || getClass() != that.getClass()) {
      return false;
    }
    MembershipRecord record = (MembershipRecord) that;
    return incarnation == record.incarnation
        && Objects.equals(member, record.member)
        && status == record.status;
  }

  @Override
  public int hashCode() {
    return Objects.hash(member, status, incarnation);
  }

  @Override
  public String toString() {
    return "{m: " + member + ", s: " + status + ", inc: " + incarnation + '}';
  }
}
