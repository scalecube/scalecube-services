package io.servicefabric.cluster;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import io.servicefabric.transport.TransportBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Anton Kharenko
 */
public class ClusterConfiguration {

	public static final int DEFAULT_PORT = 29001;
	public static final TransportBuilder.TransportSettings DEFAULT_TRANSPORT_SETTINGS = new TransportBuilder.TransportSettings();
	public static final ClusterMembershipSettings DEFAULT_CLUSTER_MEMBERSHIP_SETTINGS = new ClusterMembershipSettings();
	public static final FailureDetectorSettings DEFAULT_FAILURE_DETECTOR_SETTINGS = new FailureDetectorSettings();
	public static final GossipProtocolSettings DEFAULT_GOSSIP_PROTOCOL_SETTINGS = new GossipProtocolSettings();

	boolean autoStart = false;
	String memberId = null;
	String seedMembers = "";
	int port = DEFAULT_PORT;
	Map<String, String> metadata = new HashMap<>();
	TransportBuilder.TransportSettings transportSettings = DEFAULT_TRANSPORT_SETTINGS;
	ClusterMembershipSettings clusterMembershipSettings = DEFAULT_CLUSTER_MEMBERSHIP_SETTINGS;
	FailureDetectorSettings failureDetectorSettings = DEFAULT_FAILURE_DETECTOR_SETTINGS;
	GossipProtocolSettings gossipProtocolSettings = DEFAULT_GOSSIP_PROTOCOL_SETTINGS;

	private ClusterConfiguration() {
	}

	public static ClusterConfiguration newInstance() {
		return new ClusterConfiguration();
	}

	public void setMemberId(String memberId) {
		this.memberId = memberId;
	}

	public void setSeedMembers(String seedMembers) {
		this.seedMembers = seedMembers;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void setAutoStart(boolean autoStart) {
		this.autoStart = autoStart;
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

	public ClusterConfiguration metadata(Map<String, String> metadata) {
		setMetadata(metadata);
		return this;
	}

	public ClusterConfiguration memberId(String memberId) {
		setMemberId(memberId);
		return this;
	}

	public ClusterConfiguration seedMembers(String seedMembers) {
		setSeedMembers(seedMembers);
		return this;
	}

	public ClusterConfiguration port(int port) {
		setPort(port);
		return this;
	}

	public ClusterConfiguration autoStart(boolean autoStart) {
		setAutoStart(autoStart);
		return this;
	}

	public ClusterConfiguration clusterMembershipSettings(ClusterMembershipSettings clusterMembershipSettings) {
		setClusterMembershipSettings(clusterMembershipSettings);
		return this;
	}

	public ClusterConfiguration failureDetectorSettings(FailureDetectorSettings failureDetectorSettings) {
		setFailureDetectorSettings(failureDetectorSettings);
		return this;
	}

	public ClusterConfiguration gossipProtocolSettings(GossipProtocolSettings gossipProtocolSettings) {
		setGossipProtocolSettings(gossipProtocolSettings);
		return this;
	}

	public ClusterConfiguration transportSettings(TransportBuilder.TransportSettings transportSetting) {
		setTransportSettings(transportSetting);
		return this;
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
