package io.servicefabric.cluster.gossip.api;

import io.servicefabric.cluster.gossip.Gossip;
import rx.Observable;

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
