package io.scalecube.cluster.membership;

import io.scalecube.cluster.Member;

import rx.Observable;

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
   * Listen changes in cluster membership.
   */
  Observable<MembershipEvent> listen();

}
