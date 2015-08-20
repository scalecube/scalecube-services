package io.servicefabric.cluster;

import io.servicefabric.cluster.gossip.IGossipProtocol;
import io.servicefabric.transport.ITransport;

import java.util.List;

/**
 * @author Anton Kharenko
 */
public interface ICluster {

	ITransport transport();

	IGossipProtocol gossip();

	IClusterMembership membership();

	List<ClusterMember> members();

	ClusterMember localMember();

	ICluster join();

	void leave();

}
