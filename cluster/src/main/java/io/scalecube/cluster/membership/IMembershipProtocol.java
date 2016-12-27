package io.scalecube.cluster.membership;

import io.scalecube.cluster.Member;

import io.reactivex.Flowable;

import java.util.Map;

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
   * Updates local member metadata.
   */
  void updateMetadata(Map<String, String> metadata);

  /**
   * Updates local member metadata to set given key and value.
   */
  void updateMetadataProperty(String key, String value);

  /**
   * Listen changes in cluster membership.
   */
  Flowable<MembershipEvent> listen();

}
