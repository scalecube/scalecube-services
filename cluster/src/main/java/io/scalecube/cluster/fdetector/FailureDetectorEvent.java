package io.scalecube.cluster.fdetector;

import io.scalecube.cluster.ClusterMemberStatus;
import io.scalecube.transport.TransportEndpoint;

import javax.annotation.concurrent.Immutable;

/** Tuple class. Contains transport endpoint and its status. */
@Immutable
public final class FailureDetectorEvent {

  private final TransportEndpoint endpoint;
  private final ClusterMemberStatus status;

  private FailureDetectorEvent(TransportEndpoint endpoint, ClusterMemberStatus status) {
    this.endpoint = endpoint;
    this.status = status;
  }

  public static FailureDetectorEvent trusted(TransportEndpoint endpoint) {
    return new FailureDetectorEvent(endpoint, ClusterMemberStatus.TRUSTED);
  }

  public static FailureDetectorEvent suspected(TransportEndpoint endpoint) {
    return new FailureDetectorEvent(endpoint, ClusterMemberStatus.SUSPECTED);
  }

  public TransportEndpoint endpoint() {
    return endpoint;
  }

  public ClusterMemberStatus status() {
    return status;
  }

  @Override
  public String toString() {
    return "FailureDetectorEvent{" + "endpoint=" + endpoint + ", status=" + status + '}';
  }
}
