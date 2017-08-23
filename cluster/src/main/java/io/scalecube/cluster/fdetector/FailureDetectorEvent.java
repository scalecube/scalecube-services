package io.scalecube.cluster.fdetector;

import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.MemberStatus;

import javax.annotation.concurrent.Immutable;

/**
 * CLass contains result of ping check. */
@Immutable
public final class FailureDetectorEvent {

  private final Member member;
  private final MemberStatus status;

  FailureDetectorEvent(Member member, MemberStatus status) {
    this.member = member;
    this.status = status;
  }

  public Member member() {
    return member;
  }

  public MemberStatus status() {
    return status;
  }

  @Override
  public String toString() {
    return "FailureDetectorEvent{member=" + member + ", status=" + status + '}';
  }
}
