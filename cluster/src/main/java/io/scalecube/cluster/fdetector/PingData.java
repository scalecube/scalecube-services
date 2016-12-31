package io.scalecube.cluster.fdetector;

import io.scalecube.cluster.Member;

import io.protostuff.Tag;

/** DTO class. Supports FailureDetector messages (Ping, Ack, PingReq). */
final class PingData {
  /** Message's source address. */
  @Tag(1)
  private final Member from;
  /** Message's destination address. */
  @Tag(2)
  private final Member to;
  /** Address of member, who originally initiated ping sequence. */
  @Tag(3)
  private final Member originalIssuer;

  public PingData(Member from, Member to) {
    this.from = from;
    this.to = to;
    this.originalIssuer = null;
  }

  public PingData(Member from, Member to, Member originalIssuer) {
    this.from = from;
    this.to = to;
    this.originalIssuer = originalIssuer;
  }

  public Member getFrom() {
    return from;
  }

  public Member getTo() {
    return to;
  }

  public Member getOriginalIssuer() {
    return originalIssuer;
  }

  @Override
  public String toString() {
    return "PingData{from=" + from
        + ", to=" + to
        + (originalIssuer != null ? ", originalIssuer=" + originalIssuer : "")
        + '}';
  }
}
