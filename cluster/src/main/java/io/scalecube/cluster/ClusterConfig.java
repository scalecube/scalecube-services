package io.scalecube.cluster;

import io.scalecube.cluster.fdetector.FailureDetectorSettings;
import io.scalecube.transport.TransportSettings;

import java.util.HashMap;
import java.util.Map;

/**
 * Cluster configuration encapsulate settings needed cluster to create and successfully join.
 * 
 * @see MembershipSettings
 * @see FailureDetectorSettings
 * @see GossipProtocolSettings
 * @author Anton Kharenko
 */
public class ClusterConfig {

  public static final int DEFAULT_PORT = 4801;
  public static final int DEFAULT_PORT_COUNT = 100;
  public static final boolean DEFAULT_PORT_AUTO_INCREMENT = true;
  public static final MembershipSettings DEFAULT_CLUSTER_MEMBERSHIP_SETTINGS = new MembershipSettings();
  public static final GossipProtocolSettings DEFAULT_GOSSIP_PROTOCOL_SETTINGS = new GossipProtocolSettings();

  String seedMembers = "";
  int port = DEFAULT_PORT;
  int portCount = DEFAULT_PORT_COUNT;
  boolean portAutoIncrement = DEFAULT_PORT_AUTO_INCREMENT;
  Map<String, String> metadata = new HashMap<>();
  TransportSettings transportSettings = TransportSettings.DEFAULT;
  MembershipSettings membershipSettings = DEFAULT_CLUSTER_MEMBERSHIP_SETTINGS;
  FailureDetectorSettings failureDetectorSettings = FailureDetectorSettings.DEFAULT;
  GossipProtocolSettings gossipProtocolSettings = DEFAULT_GOSSIP_PROTOCOL_SETTINGS;

  private ClusterConfig() {}

  public static ClusterConfig newInstance() {
    return new ClusterConfig();
  }

  public void setSeedMembers(String seedMembers) {
    this.seedMembers = seedMembers;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public void setPortCount(int portCount) {
    this.portCount = portCount;
  }

  public void setPortAutoIncrement(boolean portAutoIncrement) {
    this.portAutoIncrement = portAutoIncrement;
  }

  public void setMetadata(Map<String, String> metadata) {
    this.metadata = metadata;
  }

  public void setMembershipSettings(MembershipSettings membershipSettings) {
    this.membershipSettings = membershipSettings;
  }

  public void setFailureDetectorSettings(FailureDetectorSettings failureDetectorSettings) {
    this.failureDetectorSettings = failureDetectorSettings;
  }

  public void setGossipProtocolSettings(GossipProtocolSettings gossipProtocolSettings) {
    this.gossipProtocolSettings = gossipProtocolSettings;
  }

  public void setTransportSettings(TransportSettings transportSettings) {
    this.transportSettings = transportSettings;
  }

  public ClusterConfig metadata(Map<String, String> metadata) {
    setMetadata(metadata);
    return this;
  }

  public ClusterConfig seedMembers(String seedMembers) {
    setSeedMembers(seedMembers);
    return this;
  }

  public ClusterConfig port(int port) {
    setPort(port);
    return this;
  }

  public ClusterConfig portCount(int portCount) {
    setPortCount(portCount);
    return this;
  }

  public ClusterConfig portAutoIncrement(boolean portAutoIncrement) {
    setPortAutoIncrement(portAutoIncrement);
    return this;
  }

  public ClusterConfig membershipSettings(MembershipSettings membershipSettings) {
    setMembershipSettings(membershipSettings);
    return this;
  }

  public ClusterConfig failureDetectorSettings(FailureDetectorSettings failureDetectorSettings) {
    setFailureDetectorSettings(failureDetectorSettings);
    return this;
  }

  public ClusterConfig gossipProtocolSettings(GossipProtocolSettings gossipProtocolSettings) {
    setGossipProtocolSettings(gossipProtocolSettings);
    return this;
  }

  public ClusterConfig transportSettings(TransportSettings transportSetting) {
    setTransportSettings(transportSetting);
    return this;
  }

  @Override
  public String toString() {
    return "ClusterConfiguration{"
        + ", seedMembers='" + seedMembers + '\''
        + ", port=" + port
        + ", portCount=" + portCount
        + ", portAutoIncrement=" + portAutoIncrement
        + ", metadata=" + metadata
        + ", transportSettings=" + transportSettings
        + ", clusterMembershipSettings=" + membershipSettings
        + ", failureDetectorSettings=" + failureDetectorSettings
        + ", gossipProtocolSettings=" + gossipProtocolSettings
        + '}';
  }

  public static class MembershipSettings {

    public static final int DEFAULT_SYNC_TIME = 30 * 1000;
    public static final int DEFAULT_SYNC_TIMEOUT = 3 * 1000;
    public static final int DEFAULT_MAX_SUSPECT_TIME = 60 * 1000;
    public static final int DEFAULT_MAX_SHUTDOWN_TIME = 60 * 1000;
    public static final String DEFAULT_SYNC_GROUP = "default";

    private int syncTime = DEFAULT_SYNC_TIME;
    private int syncTimeout = DEFAULT_SYNC_TIMEOUT;
    private int maxSuspectTime = DEFAULT_MAX_SUSPECT_TIME;
    private int maxShutdownTime = DEFAULT_MAX_SHUTDOWN_TIME;
    private String syncGroup = DEFAULT_SYNC_GROUP;

    public MembershipSettings() {}

    /**
     * Creates new cluster membership settings
     * 
     * @param syncTime time interval in milliseconds between two sync messages.
     * @param syncTimeout waiting time in milliseconds for the response to sync message.
     * @param maxSuspectTime waiting time interval in milliseconds after suspected event when node will not be removed
     * @param maxShutdownTime waiting time interval in milliseconds after shutdown event when node will not be removed
     * @param syncGroup cluster's sync group. Members with different groups will form different clusters.
     */
    public MembershipSettings(int syncTime, int syncTimeout, int maxSuspectTime, int maxShutdownTime,
                              String syncGroup) {
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
      return "ClusterMembershipSettings{syncTime=" + syncTime
          + ", syncTimeout=" + syncTimeout
          + ", maxSuspectTime=" + maxSuspectTime
          + ", maxShutdownTime=" + maxShutdownTime
          + ", syncGroup='" + syncGroup + '\''
          + '}';
    }
  }

  public static class GossipProtocolSettings {

    public static final int DEFAULT_MAX_GOSSIP_SENT = 2;
    public static final int DEFAULT_GOSSIP_TIME = 300;
    public static final int DEFAULT_MAX_ENDPOINTS_TO_SELECT = 3;

    private int maxGossipSent = DEFAULT_MAX_GOSSIP_SENT;
    private int gossipTime = DEFAULT_GOSSIP_TIME;
    private int maxEndpointsToSelect = DEFAULT_MAX_ENDPOINTS_TO_SELECT;

    public GossipProtocolSettings() {}

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
      return "GossipProtocolSettings{maxGossipSent=" + maxGossipSent
          + ", gossipTime=" + gossipTime
          + ", maxEndpointsToSelect=" + maxEndpointsToSelect
          + '}';
    }
  }

}
