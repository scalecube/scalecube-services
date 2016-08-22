package io.scalecube.cluster;

import io.scalecube.transport.Address;
import io.scalecube.transport.IListenable;

import rx.Observable;
import rx.Scheduler;

import java.util.List;

/**
 * Cluster Membership Protocol component responsible for managing information about existing members of the cluster.
 *
 * @author Anton Kharenko
 */
public interface IClusterMembership extends IListenable<ClusterMember> {

  /**
   * Returns current cluster members list.
   */
  List<ClusterMember> members();

  /**
   * Returns cluster member by its id or null if no member with such id exists.
   */
  ClusterMember member(String id);

  /**
   * Returns cluster member by its address or null if no member with such address exists.
   */
  ClusterMember member(Address address);

  /**
   * Returns local cluster member.
   */
  ClusterMember localMember();

  /**
   * Check if a given member is a local member return true in case the ClusterMember is local to the cluster instance.
   *
   * @param member checks if member is local
   * @return True if the given cluster member is a local cluster member; false otherwise
   */
  boolean isLocalMember(ClusterMember member);

  /**
   * Listen status updates on registered cluster members (except local one).
   */
  @Override
  Observable<ClusterMember> listen();

  /**
   * Listen status updates on registered cluster members (except local one).
   */
  @Override
  Observable<ClusterMember> listen(Scheduler scheduler);
}
