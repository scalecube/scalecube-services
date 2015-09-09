package io.servicefabric.cluster;

import rx.Observable;

import java.util.List;

/**
 * Cluster Membership Protocol component responsible for managing information about existing members of the cluster.
 *
 * @author Anton Kharenko
 */
public interface IClusterMembership {

  /** Returns current cluster members list. */
  List<ClusterMember> members();

  /** Returns local cluster member. */
  ClusterMember localMember();

  /** Listen status updates on registered cluster members (except local one). */
  Observable<ClusterMember> listenUpdates();

  /**
   * check if a given member is a local member return true in case the ClusterMember is local to the cluster instance
   * 
   * @param ClusterMember
   * @return true if this cluster memeber is a local cluster memeber
   * @author ronen hamias
   */
  boolean isLocalMember(ClusterMember member);
}
