package io.servicefabric.cluster;

import static com.google.common.base.Preconditions.checkArgument;
import io.servicefabric.cluster.api.ICluster;
import io.servicefabric.cluster.fdetector.FailureDetector;
import io.servicefabric.cluster.gossip.GossipProtocol;
import io.servicefabric.transport.Transport;
import io.servicefabric.transport.TransportBuilder;

import java.util.HashMap;
import java.util.Map;

import rx.schedulers.Schedulers;

/**
 * @author Anton Kharenko
 */
public class ClusterBuilder {

	// Mandatory parameters
	private final ClusterEndpoint localClusterEndpoint;
	private final String wellknownMembers;

	// Optional parameters
	private Map<String, String> metadata = new HashMap<>();
	private TransportBuilder.TransportSettings transportSettings = null;
	private ClusterMembershipSettings clusterMembershipSettings = null;
	private FailureDetectorSettings failureDetectorSettings = null;
	private GossipProtocolSettings gossipProtocolSettings = null;

	private ClusterBuilder(ClusterEndpoint localClusterEndpoint, String wellknownMembers) {
		checkArgument(localClusterEndpoint != null);
		checkArgument(wellknownMembers != null);
		this.localClusterEndpoint = localClusterEndpoint;
		this.wellknownMembers = wellknownMembers;
	}

	public static ClusterBuilder newInstance(ClusterEndpoint localClusterEndpoint, String wellknownMembers) {
		return new ClusterBuilder(localClusterEndpoint, wellknownMembers);
	}

	public void setMetadata(Map<String, String> metadata) {
		this.metadata = metadata;
	}

	public void setClusterMembershipSettings(ClusterMembershipSettings clusterMembershipSettings) {
		this.clusterMembershipSettings = clusterMembershipSettings;
	}

	public void setFailureDetectorSettings(FailureDetectorSettings failureDetectorSettings) {
		this.failureDetectorSettings = failureDetectorSettings;
	}

	public void setGossipProtocolSettings(GossipProtocolSettings gossipProtocolSettings) {
		this.gossipProtocolSettings = gossipProtocolSettings;
	}

	public void setTransportSettings(TransportBuilder.TransportSettings transportSettings) {
		this.transportSettings = transportSettings;
	}

	public ClusterBuilder metadata(Map<String, String> metadata) {
		setMetadata(metadata);
		return this;
	}

	public ClusterBuilder clusterMembershipSettings(ClusterMembershipSettings clusterMembershipSettings) {
		setClusterMembershipSettings(clusterMembershipSettings);
		return this;
	}

	public ClusterBuilder failureDetectorSettings(FailureDetectorSettings failureDetectorSettings) {
		setFailureDetectorSettings(failureDetectorSettings);
		return this;
	}

	public ClusterBuilder gossipProtocolSettings(GossipProtocolSettings gossipProtocolSettings) {
		setGossipProtocolSettings(gossipProtocolSettings);
		return this;
	}

	public ClusterBuilder transportSettings(TransportBuilder.TransportSettings transportSetting) {
		setTransportSettings(transportSetting);
		return this;
	}

	public ICluster build() {
		// Build transport
		TransportBuilder transportBuilder = TransportBuilder.newInstance(localClusterEndpoint.endpoint(), localClusterEndpoint.endpointId());
		if (transportSettings != null) {
			transportBuilder.setTransportSettings(transportSettings);
		}
		Transport transport = (Transport) transportBuilder.build();

		// Build gossip protocol component
		GossipProtocol gossipProtocol = new GossipProtocol(localClusterEndpoint);
		gossipProtocol.setTransport(transport);
		if (gossipProtocolSettings != null) {
			gossipProtocol.setMaxGossipSent(gossipProtocolSettings.getMaxGossipSent());
			gossipProtocol.setGossipTime(gossipProtocolSettings.getGossipTime());
			gossipProtocol.setMaxEndpointsToSelect(gossipProtocolSettings.getMaxEndpointsToSelect());
		} else {
			gossipProtocol.setMaxGossipSent(GossipProtocolSettings.DEFAULT_MAX_GOSSIP_SENT);
			gossipProtocol.setGossipTime(GossipProtocolSettings.DEFAULT_GOSSIP_TIME);
			gossipProtocol.setMaxEndpointsToSelect(GossipProtocolSettings.DEFAULT_MAX_ENDPOINTS_TO_SELECT);
		}

		// Build failure detector component
		FailureDetector failureDetector = new FailureDetector(localClusterEndpoint, Schedulers.from(transport.getEventExecutor()));
		failureDetector.setTransport(transport);
		if (failureDetectorSettings != null) {
			failureDetector.setPingTime(failureDetectorSettings.getPingTime());
			failureDetector.setPingTimeout(failureDetectorSettings.getPingTimeout());
			failureDetector.setMaxEndpointsToSelect(failureDetectorSettings.getMaxEndpointsToSelect());
		} else {
			failureDetector.setPingTime(FailureDetectorSettings.DEFAULT_PING_TIME);
			failureDetector.setPingTimeout(FailureDetectorSettings.DEFAULT_PING_TIMEOUT);
			failureDetector.setMaxEndpointsToSelect(FailureDetectorSettings.DEFAULT_MAX_ENDPOINTS_TO_SELECT);
		}

		// Build cluster membership component
		ClusterMembership clusterMembership = new ClusterMembership(localClusterEndpoint, Schedulers.from(transport.getEventExecutor()));
		clusterMembership.setFailureDetector(failureDetector);
		clusterMembership.setGossipProtocol(gossipProtocol);
		clusterMembership.setTransport(transport);
		clusterMembership.setLocalMetadata(metadata);
		clusterMembership.setWellknownMembers(wellknownMembers);
		if (clusterMembershipSettings != null) {
			clusterMembership.setSyncTime(clusterMembershipSettings.getSyncTime());
			clusterMembership.setSyncTimeout(clusterMembershipSettings.getSyncTimeout());
			clusterMembership.setMaxSuspectTime(clusterMembershipSettings.getMaxSuspectTime());
			clusterMembership.setMaxShutdownTime(clusterMembershipSettings.getMaxShutdownTime());
			clusterMembership.setSyncGroup(clusterMembershipSettings.getSyncGroup());
		} else {
			clusterMembership.setSyncTime(ClusterMembershipSettings.DEFAULT_SYNC_TIME);
			clusterMembership.setSyncTimeout(ClusterMembershipSettings.DEFAULT_SYNC_TIMEOUT);
			clusterMembership.setMaxSuspectTime(ClusterMembershipSettings.DEFAULT_MAX_SUSPECT_TIME);
			clusterMembership.setMaxShutdownTime(ClusterMembershipSettings.DEFAULT_MAX_SHUTDOWN_TIME);
			clusterMembership.setSyncGroup(ClusterMembershipSettings.DEFAULT_SYNC_GROUP);
		}

		// Build cluster component
		return new Cluster(transport, failureDetector, gossipProtocol, clusterMembership);
	}

	public static class ClusterMembershipSettings {

		public static final int DEFAULT_SYNC_TIME = 10 * 1000;
		public static final int DEFAULT_SYNC_TIMEOUT = 3 * 1000;
		public static final int DEFAULT_MAX_SUSPECT_TIME = 60 * 1000;
		public static final int DEFAULT_MAX_SHUTDOWN_TIME = 60 * 1000;
		public static final String DEFAULT_SYNC_GROUP = "default";

		private int syncTime = DEFAULT_SYNC_TIME;
		private int syncTimeout = DEFAULT_SYNC_TIMEOUT;
		private int maxSuspectTime = DEFAULT_MAX_SUSPECT_TIME;
		private int maxShutdownTime = DEFAULT_MAX_SHUTDOWN_TIME;
		private String syncGroup = DEFAULT_SYNC_GROUP;

		public ClusterMembershipSettings() {
		}

		public ClusterMembershipSettings(int syncTime, int syncTimeout, int maxSuspectTime, int maxShutdownTime, String syncGroup) {
			this.syncTime = syncTime;
			this.syncTimeout = syncTimeout;
			this.maxSuspectTime = maxSuspectTime;
			this.maxShutdownTime = maxShutdownTime;
			this.syncGroup = syncGroup;
		}

		public int getSyncTime() {
			return syncTime;
		}

		public void setSyncTime(int syncTime) {
			this.syncTime = syncTime;
		}

		public int getSyncTimeout() {
			return syncTimeout;
		}

		public void setSyncTimeout(int syncTimeout) {
			this.syncTimeout = syncTimeout;
		}

		public int getMaxSuspectTime() {
			return maxSuspectTime;
		}

		public void setMaxSuspectTime(int maxSuspectTime) {
			this.maxSuspectTime = maxSuspectTime;
		}

		public int getMaxShutdownTime() {
			return maxShutdownTime;
		}

		public void setMaxShutdownTime(int maxShutdownTime) {
			this.maxShutdownTime = maxShutdownTime;
		}

		public String getSyncGroup() {
			return syncGroup;
		}

		public void setSyncGroup(String syncGroup) {
			this.syncGroup = syncGroup;
		}

		@Override
		public String toString() {
			return "ClusterMembershipSettings{" +
					"syncTime=" + syncTime +
					", syncTimeout=" + syncTimeout +
					", maxSuspectTime=" + maxSuspectTime +
					", maxShutdownTime=" + maxShutdownTime +
					", syncGroup='" + syncGroup + '\'' +
					'}';
		}
	}

	public static class GossipProtocolSettings {

		public static final int DEFAULT_MAX_GOSSIP_SENT = 2;
		public static final int DEFAULT_GOSSIP_TIME = 300;
		public static final int DEFAULT_MAX_ENDPOINTS_TO_SELECT = 3;

		private int maxGossipSent = DEFAULT_MAX_GOSSIP_SENT;
		private int gossipTime = DEFAULT_GOSSIP_TIME;
		private int maxEndpointsToSelect = DEFAULT_MAX_ENDPOINTS_TO_SELECT;

		public GossipProtocolSettings() {
		}

		public GossipProtocolSettings(int maxGossipSent, int gossipTime, int maxEndpointsToSelect) {
			this.maxGossipSent = maxGossipSent;
			this.gossipTime = gossipTime;
			this.maxEndpointsToSelect = maxEndpointsToSelect;
		}

		public int getMaxGossipSent() {
			return maxGossipSent;
		}

		public void setMaxGossipSent(int maxGossipSent) {
			this.maxGossipSent = maxGossipSent;
		}

		public int getGossipTime() {
			return gossipTime;
		}

		public void setGossipTime(int gossipTime) {
			this.gossipTime = gossipTime;
		}

		public int getMaxEndpointsToSelect() {
			return maxEndpointsToSelect;
		}

		public void setMaxEndpointsToSelect(int maxEndpointsToSelect) {
			this.maxEndpointsToSelect = maxEndpointsToSelect;
		}

		@Override
		public String toString() {
			return "GossipProtocolSettings{" +
					"maxGossipSent=" + maxGossipSent +
					", gossipTime=" + gossipTime +
					", maxEndpointsToSelect=" + maxEndpointsToSelect +
					'}';
		}
	}

	public static class FailureDetectorSettings {

		public static final int DEFAULT_PING_TIME = 2000;
		public static final int DEFAULT_PING_TIMEOUT = 1000;
		public static final int DEFAULT_MAX_ENDPOINTS_TO_SELECT = 3;

		private int pingTime = DEFAULT_PING_TIME;
		private int pingTimeout = DEFAULT_PING_TIMEOUT;
		private int maxEndpointsToSelect = DEFAULT_MAX_ENDPOINTS_TO_SELECT;

		public FailureDetectorSettings() {
		}

		public FailureDetectorSettings(int pingTime, int pingTimeout, int maxEndpointsToSelect) {
			this.pingTime = pingTime;
			this.pingTimeout = pingTimeout;
			this.maxEndpointsToSelect = maxEndpointsToSelect;
		}

		public int getPingTime() {
			return pingTime;
		}

		public void setPingTime(int pingTime) {
			this.pingTime = pingTime;
		}

		public int getPingTimeout() {
			return pingTimeout;
		}

		public void setPingTimeout(int pingTimeout) {
			this.pingTimeout = pingTimeout;
		}

		public int getMaxEndpointsToSelect() {
			return maxEndpointsToSelect;
		}

		public void setMaxEndpointsToSelect(int maxEndpointsToSelect) {
			this.maxEndpointsToSelect = maxEndpointsToSelect;
		}

		@Override
		public String toString() {
			return "FailureDetectorSettings{" +
					"pingTime=" + pingTime +
					", pingTimeout=" + pingTimeout +
					", maxEndpointsToSelect=" + maxEndpointsToSelect +
					'}';
		}
	}

}
