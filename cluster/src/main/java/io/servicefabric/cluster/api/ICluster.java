package io.servicefabric.cluster.api;

import io.servicefabric.cluster.ClusterMember;
import io.servicefabric.cluster.gossip.api.IGossipProtocol;
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

	void start();

	void stop();

}
