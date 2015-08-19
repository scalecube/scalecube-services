package io.servicefabric.cluster;

import io.servicefabric.cluster.fdetector.IFailureDetector;
import io.servicefabric.cluster.gossip.IGossipProtocol;
import io.servicefabric.transport.ITransport;

import java.util.List;

/**
 * @author Anton Kharenko
 */
class Cluster implements ICluster {

	private final ITransport transport;
	private final IFailureDetector failureDetector;
	private final IGossipProtocol gossipProtocol;
	private final IClusterMembership clusterMembership;

	protected Cluster(ITransport transport, IFailureDetector failureDetector, IGossipProtocol gossipProtocol,
			IClusterMembership clusterMembership) {
		this.transport = transport;
		this.failureDetector = failureDetector;
		this.gossipProtocol = gossipProtocol;
		this.clusterMembership = clusterMembership;
	}

	@Override
	public ITransport transport() {
		return transport;
	}

	@Override
	public IGossipProtocol gossip() {
		return gossipProtocol;
	}

	@Override
	public IClusterMembership membership() {
		return clusterMembership;
	}

	@Override
	public List<ClusterMember> members() {
		return membership().members();
	}

	@Override
	public ClusterMember localMember() {
		return membership().localMember();
	}

	@Override
	public void start() {
		transport.start();
		failureDetector.start();
		gossipProtocol.start();
		clusterMembership.start();
	}

	@Override
	public void stop() {
		clusterMembership.stop();
		gossipProtocol.stop();
		failureDetector.stop();
		transport.stop();
	}
}
