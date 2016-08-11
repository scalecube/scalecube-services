package io.scalecube.cluster.fdetector;

import io.scalecube.transport.Address;

import io.protostuff.Tag;

/** DTO class. Supports FailureDetector messages (Ping, Ack, PingReq). */
final class PingData {
  /** Message's source address. */
  @Tag(1)
  private Address from;
  /** Message's destination address. */
  @Tag(2)
  private Address to;
  /** Address of member, who originally initiated ping sequence. */
  @Tag(3)
  private Address originalIssuer;

  public PingData(Address from, Address to) {
    this.from = from;
    this.to = to;
  }

  public PingData(Address from, Address to, Address originalIssuer) {
    this.from = from;
    this.to = to;
    this.originalIssuer = originalIssuer;
  }

  public Address getFrom() {
    return from;
  }

  public Address getTo() {
    return to;
  }

  public Address getOriginalIssuer() {
    return originalIssuer;
  }

  @Override
  public String toString() {
    return "FailureDetectorData{" + ", from=" + from + ", to=" + to + ", originalIssuer=" + originalIssuer + '}';
  }
}
