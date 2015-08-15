package io.servicefabric.cluster;

import java.util.List;

import rx.Observable;

public interface IClusterMembership {

	void start();

	void stop();

	/** Returns current cluster members list. */
	List<ClusterMember> members();

	/** Returns local cluster member. */
	ClusterMember localMember();

	/** Listen status updates on registered cluster members (except local one). */
	Observable<ClusterMember> listenUpdates();

	/** Denoting fact that local member is getting gracefully shutdown. */
	void publishShutdown();
}
