package io.scalecube.cluster.fdetector;

import io.scalecube.cluster.membership.MemberStatus;
import io.scalecube.transport.Address;

import javax.annotation.concurrent.Immutable;

/** Tuple class. Contains member address and its status. */
@Immutable
public final class FailureDetectorEvent {

  private final Address address;
  private final MemberStatus status;

  FailureDetectorEvent(Address address, MemberStatus status) {
    this.address = address;
    this.status = status;
  }

  public Address address() {
    return address;
  }

  public MemberStatus status() {
    return status;
  }

  @Override
  public String toString() {
    return "FailureDetectorEvent{address=" + address + ", status=" + status + '}';
  }
}
