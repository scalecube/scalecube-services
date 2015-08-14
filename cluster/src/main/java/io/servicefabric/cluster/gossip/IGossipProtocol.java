package io.servicefabric.cluster.gossip;

import java.util.Collection;

import rx.Observable;

import io.servicefabric.cluster.ClusterEndpoint;

/**
 * Gossip Protocol component provides generic solution for spreading information (gossips) over the cluster
 * members endpoints using infection-style information dissemination algorithms.
 */
public interface IGossipProtocol {

	void start();

	void stop();

	/** Spreads given gossip through the cluster. */
	void spread(String qualifier, Object data);

	/** Listens for all gossips inside the cluster. */
	Observable<Gossip> listen();

}
