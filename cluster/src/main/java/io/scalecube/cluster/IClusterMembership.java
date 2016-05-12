package io.scalecube.cluster;

import java.util.List;

import rx.Observable;

/**
 * Cluster Membership Protocol component responsible for managing information about existing members of the cluster.
 *
 * @author Anton Kharenko
 */
public interface IClusterMembership {

  /** Returns current cluster members list. */
  List<ClusterMember> members();

  /** Returns cluster member by its id or null if no member with such id exists. */
  ClusterMember member(String id);

  /** Returns local cluster member. */
  ClusterMember localMember();

  /** Listen status updates on registered cluster members (except local one). */
  Observable<ClusterMember> listenUpdates();

  /**
   * Check if a given member is a local member return true in case the ClusterMember is local to the cluster instance.
   *
   * @param member checks if member is local
   * @return True if the given cluster member is a local cluster member; false otherwise
   */
  boolean isLocalMember(ClusterMember member);

}
