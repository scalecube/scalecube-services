package io.scalecube.cluster.membership;

import io.scalecube.cluster.Member;
import io.scalecube.cluster.MembershipEvent;
import io.scalecube.transport.Address;

import rx.Observable;

import java.util.List;

/**
 * Cluster Membership Protocol component responsible for managing information about existing members of the cluster.
 *
 * @author Anton Kharenko
 */
public interface IMembershipProtocol {

  /**
   * Returns local cluster member.
   */
  Member member();

  /**
   * Returns cluster member by its id or null if no member with such id exists.
   */
  Member member(String id);

  /**
   * Returns cluster member by its address or null if no member with such address exists.
   */
  Member member(Address address);

  /**
   * Returns current cluster members list.
   */
  List<Member> members();

  /**
   * Returns current cluster members list excluding local member.
   */
  List<Member> otherMembers();

  /**
   * Listen changes in cluster membership.
   */
  Observable<MembershipEvent> listen();

}
