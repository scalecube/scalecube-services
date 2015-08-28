package io.servicefabric.cluster;

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

	/** Returns local cluster member. */
	ClusterMember localMember();

	/** Listen status updates on registered cluster members (except local one). */
	Observable<ClusterMember> listenUpdates();

}
