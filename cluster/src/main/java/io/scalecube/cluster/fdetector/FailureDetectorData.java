package io.scalecube.cluster.fdetector;

import io.scalecube.transport.TransportEndpoint;

import io.protostuff.Tag;

/** DTO class. Supports FailureDetector messages (Ping, Ack, PingReq). */
final class FailureDetectorData {
  /** Message's source endpoint. */
  @Tag(1)
  private TransportEndpoint from;
  /** Message's destination endpoint. */
  @Tag(2)
  private TransportEndpoint to;
  /** Endpoint, who originally initiated ping sequence. */
  @Tag(3)
  private TransportEndpoint originalIssuer;

  public FailureDetectorData(TransportEndpoint from, TransportEndpoint to) {
    this.from = from;
    this.to = to;
  }

  public FailureDetectorData(TransportEndpoint from, TransportEndpoint to, TransportEndpoint originalIssuer) {
    this.from = from;
    this.to = to;
    this.originalIssuer = originalIssuer;
  }

  public TransportEndpoint getFrom() {
    return from;
  }

  public TransportEndpoint getTo() {
    return to;
  }

  public TransportEndpoint getOriginalIssuer() {
    return originalIssuer;
  }

  @Override
  public String toString() {
    return "FailureDetectorData{" + ", from=" + from + ", to=" + to + ", originalIssuer=" + originalIssuer + '}';
  }
}
