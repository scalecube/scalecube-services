package io.scalecube.cluster.membership;

import io.scalecube.transport.Address;

import rx.Observable;

import java.util.List;

/**
 * Cluster Membership Protocol component responsible for managing information about existing members of the cluster.
 *
 * @author Anton Kharenko
 */
public interface IMembershipProtocol {

  /** Returns current cluster members list. */
  List<MembershipRecord> members();

  /** Returns current cluster members list excluding local member. */
  List<MembershipRecord> otherMembers();

  /** Returns cluster member by its id or null if no member with such id exists. */
  MembershipRecord member(String id);

  /** Returns cluster member by its address or null if no member with such address exists. */
  MembershipRecord member(Address address);

  /** Returns local cluster member. */
  MembershipRecord localMember();

  /** Listen status updates on registered cluster members (except local one). */
  Observable<MembershipRecord> listenUpdates();

}
