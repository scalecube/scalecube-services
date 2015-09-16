package io.servicefabric.cluster.fdetector;

import javax.annotation.concurrent.Immutable;

import static io.servicefabric.cluster.ClusterMemberStatus.SUSPECTED;
import static io.servicefabric.cluster.ClusterMemberStatus.TRUSTED;

import io.servicefabric.transport.TransportEndpoint;
import io.servicefabric.cluster.ClusterMemberStatus;

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
    return new FailureDetectorEvent(endpoint, TRUSTED);
  }

  public static FailureDetectorEvent suspected(TransportEndpoint endpoint) {
    return new FailureDetectorEvent(endpoint, SUSPECTED);
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
