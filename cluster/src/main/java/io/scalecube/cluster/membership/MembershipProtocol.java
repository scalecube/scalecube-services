package io.scalecube.cluster.membership;

import io.scalecube.cluster.Member;

import rx.Observable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Cluster Membership Protocol component responsible for managing information about existing members of the cluster.
 *
 * @author Anton Kharenko
 */
public interface MembershipProtocol {

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
  Observable<MembershipEvent> listen();

}
