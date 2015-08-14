package io.servicefabric.cluster;

import rx.Observable;

import java.util.List;

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
