package io.scalecube.cluster;

import io.scalecube.cluster.fdetector.FailureDetectorConfig;
import io.scalecube.cluster.gossip.GossipProtocolConfig;
import io.scalecube.transport.TransportConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * Cluster configuration encapsulate settings needed cluster to create and successfully join.
 * 
 * @see MembershipConfig
 * @see FailureDetectorConfig
 * @see GossipProtocolConfig
 *
 * @author Anton Kharenko
 */
public class ClusterConfig {

  public static final MembershipConfig DEFAULT_MEMBERSHIP_CONFIG = new MembershipConfig();

  String seedMembers = "";
  Map<String, String> metadata = new HashMap<>();
  TransportConfig transportConfig = TransportConfig.DEFAULT;
  MembershipConfig membershipConfig = DEFAULT_MEMBERSHIP_CONFIG;
  FailureDetectorConfig failureDetectorConfig = FailureDetectorConfig.DEFAULT;
  GossipProtocolConfig gossipProtocolConfig = GossipProtocolConfig.DEFAULT;

  private ClusterConfig() {}

  public static ClusterConfig newInstance() {
    return new ClusterConfig();
  }

  public void setSeedMembers(String seedMembers) {
    this.seedMembers = seedMembers;
  }

  public void setMetadata(Map<String, String> metadata) {
    this.metadata = metadata;
  }

  public void setMembershipConfig(MembershipConfig membershipConfig) {
    this.membershipConfig = membershipConfig;
  }

  public void setFailureDetectorConfig(FailureDetectorConfig failureDetectorConfig) {
    this.failureDetectorConfig = failureDetectorConfig;
  }

  public void setGossipProtocolConfig(GossipProtocolConfig gossipProtocolConfig) {
    this.gossipProtocolConfig = gossipProtocolConfig;
  }

  public void setTransportConfig(TransportConfig transportConfig) {
    this.transportConfig = transportConfig;
  }

  public ClusterConfig metadata(Map<String, String> metadata) {
    setMetadata(metadata);
    return this;
  }

  public ClusterConfig seedMembers(String seedMembers) {
    setSeedMembers(seedMembers);
    return this;
  }

  public ClusterConfig membershipConfig(MembershipConfig membershipConfig) {
    setMembershipConfig(membershipConfig);
    return this;
  }

  public ClusterConfig failureDetectorConfig(FailureDetectorConfig failureDetectorConfig) {
    setFailureDetectorConfig(failureDetectorConfig);
    return this;
  }

  public ClusterConfig gossipProtocolConfig(GossipProtocolConfig gossipProtocolConfig) {
    setGossipProtocolConfig(gossipProtocolConfig);
    return this;
  }

  public ClusterConfig transportConfig(TransportConfig transportSetting) {
    setTransportConfig(transportSetting);
    return this;
  }

  @Override
  public String toString() {
    return "ClusterConfig{seedMembers='" + seedMembers + '\''
        + ", metadata=" + metadata
        + ", transportConfig=" + transportConfig
        + ", membershipConfig=" + membershipConfig
        + ", failureDetectorConfig=" + failureDetectorConfig
        + ", gossipProtocolConfig=" + gossipProtocolConfig
        + '}';
  }

  public static class MembershipConfig {

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

    public MembershipConfig() {}

    /**
     * Creates new cluster membership settings
     * 
     * @param syncTime time interval in milliseconds between two sync messages.
     * @param syncTimeout waiting time in milliseconds for the response to sync message.
     * @param maxSuspectTime waiting time interval in milliseconds after suspected event when node will not be removed
     * @param maxShutdownTime waiting time interval in milliseconds after shutdown event when node will not be removed
     * @param syncGroup cluster's sync group. Members with different groups will form different clusters.
     */
    public MembershipConfig(int syncTime, int syncTimeout, int maxSuspectTime, int maxShutdownTime,
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
      return "MembershipConfigF{syncTime=" + syncTime
          + ", syncTimeout=" + syncTimeout
          + ", maxSuspectTime=" + maxSuspectTime
          + ", maxShutdownTime=" + maxShutdownTime
          + ", syncGroup='" + syncGroup + '\''
          + '}';
    }
  }

}
