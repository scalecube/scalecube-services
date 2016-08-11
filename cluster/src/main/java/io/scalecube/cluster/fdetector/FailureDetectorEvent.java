package io.scalecube.cluster.fdetector;

import io.scalecube.cluster.ClusterMemberStatus;
import io.scalecube.transport.Address;

import javax.annotation.concurrent.Immutable;

/** Tuple class. Contains member address and its status. */
@Immutable
public final class FailureDetectorEvent {

  private final Address address;
  private final ClusterMemberStatus status;

  FailureDetectorEvent(Address address, ClusterMemberStatus status) {
    this.address = address;
    this.status = status;
  }

  public Address address() {
    return address;
  }

  public ClusterMemberStatus status() {
    return status;
  }

  @Override
  public String toString() {
    return "FailureDetectorEvent{address=" + address + ", status=" + status + '}';
  }
}
