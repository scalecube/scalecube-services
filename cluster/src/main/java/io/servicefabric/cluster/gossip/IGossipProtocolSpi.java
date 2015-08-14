package io.servicefabric.cluster.gossip;

import io.servicefabric.cluster.ClusterEndpoint;

import java.util.Collection;

/**
 * @author Anton Kharenko
 */
public interface IGossipProtocolSpi extends IGossipProtocol {

	/** Updates list of cluster members among which should be spread gossips. */
	void setClusterMembers(Collection<ClusterEndpoint> endpoints);

}
