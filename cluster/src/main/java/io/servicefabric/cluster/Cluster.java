package io.servicefabric.cluster;

import com.google.common.base.Preconditions;
import io.servicefabric.cluster.fdetector.FailureDetector;
import io.servicefabric.cluster.fdetector.IFailureDetector;
import io.servicefabric.cluster.gossip.GossipProtocol;
import io.servicefabric.cluster.gossip.IGossipProtocol;
import io.servicefabric.transport.ITransport;
import io.servicefabric.transport.Transport;
import io.servicefabric.transport.TransportBuilder;
import io.servicefabric.transport.TransportEndpoint;
import rx.schedulers.Schedulers;

import java.util.List;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Anton Kharenko
 */
public class Cluster implements ICluster {

	private final ITransport transport;
	private final IFailureDetector failureDetector;
	private final IGossipProtocol gossipProtocol;
	private final IClusterMembership clusterMembership;

	private Cluster(ITransport transport, IFailureDetector failureDetector, IGossipProtocol gossipProtocol,
			IClusterMembership clusterMembership) {
		this.transport = transport;
		this.failureDetector = failureDetector;
		this.gossipProtocol = gossipProtocol;
		this.clusterMembership = clusterMembership;
	}

	public static ICluster newInstance() {
		return newInstance(ClusterConfiguration.newInstance());
	}

	public static ICluster newInstance(int port) {
		return newInstance(
				ClusterConfiguration.newInstance()
						.port(port)
		);
	}

	public static ICluster newInstance(int port, String seedMembers) {
		return newInstance(
				ClusterConfiguration.newInstance()
						.port(port)
						.seedMembers(seedMembers)
		);
	}

	public static ICluster newInstance(String memberId, int port, String seedMembers) {
		return newInstance(
				ClusterConfiguration.newInstance()
						.memberId(memberId)
						.port(port)
						.seedMembers(seedMembers)
		);
	}

	public static ICluster newInstance(ClusterConfiguration config) {
		Preconditions.checkNotNull(config);
		checkNotNull(config.transportSettings);
		checkNotNull(config.gossipProtocolSettings);
		checkNotNull(config.failureDetectorSettings);
		checkNotNull(config.clusterMembershipSettings);

		// Build local endpoint
		String memberId = config.memberId != null ? config.memberId : UUID.randomUUID().toString();
		ClusterEndpoint localClusterEndpoint = ClusterEndpoint.from(memberId, TransportEndpoint.localTcp(config.port, 0));

		// Build transport
		TransportBuilder transportBuilder = TransportBuilder.newInstance(localClusterEndpoint.endpoint(), localClusterEndpoint.endpointId());
		transportBuilder.setTransportSettings(config.transportSettings);
		Transport transport = (Transport) transportBuilder.build();

		// Build gossip protocol component
		GossipProtocol gossipProtocol = new GossipProtocol(localClusterEndpoint);
		gossipProtocol.setTransport(transport);
		gossipProtocol.setMaxGossipSent(config.gossipProtocolSettings.getMaxGossipSent());
		gossipProtocol.setGossipTime(config.gossipProtocolSettings.getGossipTime());
		gossipProtocol.setMaxEndpointsToSelect(config.gossipProtocolSettings.getMaxEndpointsToSelect());

		// Build failure detector component
		FailureDetector failureDetector = new FailureDetector(localClusterEndpoint, Schedulers.from(transport.getEventExecutor()));
		failureDetector.setTransport(transport);
		failureDetector.setPingTime(config.failureDetectorSettings.getPingTime());
		failureDetector.setPingTimeout(config.failureDetectorSettings.getPingTimeout());
		failureDetector.setMaxEndpointsToSelect(config.failureDetectorSettings.getMaxEndpointsToSelect());

		// Build cluster membership component
		ClusterMembership clusterMembership = new ClusterMembership(localClusterEndpoint, Schedulers.from(transport.getEventExecutor()));
		clusterMembership.setFailureDetector(failureDetector);
		clusterMembership.setGossipProtocol(gossipProtocol);
		clusterMembership.setTransport(transport);
		clusterMembership.setLocalMetadata(config.metadata);
		clusterMembership.setSeedMembers(config.seedMembers);
		clusterMembership.setSyncTime(config.clusterMembershipSettings.getSyncTime());
		clusterMembership.setSyncTimeout(config.clusterMembershipSettings.getSyncTimeout());
		clusterMembership.setMaxSuspectTime(config.clusterMembershipSettings.getMaxSuspectTime());
		clusterMembership.setMaxShutdownTime(config.clusterMembershipSettings.getMaxShutdownTime());
		clusterMembership.setSyncGroup(config.clusterMembershipSettings.getSyncGroup());

		// Build cluster component
		ICluster cluster = new Cluster(transport, failureDetector, gossipProtocol, clusterMembership);

		// Auto start
		if (config.autoStart) {
			cluster.start();
		}

		return cluster;
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
	public ICluster start() {
		transport.start();
		failureDetector.start();
		gossipProtocol.start();
		clusterMembership.start();
		return this;
	}

	@Override
	public void stop() {
		clusterMembership.stop();
		gossipProtocol.stop();
		failureDetector.stop();
		transport.stop();
	}
}
