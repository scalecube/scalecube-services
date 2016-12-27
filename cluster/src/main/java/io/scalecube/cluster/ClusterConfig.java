package io.scalecube.cluster;

import io.scalecube.cluster.fdetector.FailureDetectorConfig;
import io.scalecube.cluster.gossip.GossipConfig;
import io.scalecube.cluster.membership.MembershipConfig;
import io.scalecube.transport.Address;
import io.scalecube.transport.TransportConfig;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Cluster configuration encapsulate settings needed cluster to create and successfully join.
 * 
 * @see MembershipConfig
 * @see FailureDetectorConfig
 * @see GossipConfig
 * @see TransportConfig
 *
 * @author Anton Kharenko
 */
public final class ClusterConfig implements FailureDetectorConfig, GossipConfig, MembershipConfig {

  public static final int DEFAULT_SYNC_INTERVAL = 30_000;
  public static final int DEFAULT_SYNC_TIMEOUT = 1_000;
  public static final int DEFAULT_SUSPECT_TIMEOUT = 3_000;
  public static final String DEFAULT_SYNC_GROUP = "default";

  public static final int DEFAULT_PING_INTERVAL = 1000;
  public static final int DEFAULT_PING_TIMEOUT = 500;
  public static final int DEFAULT_PING_REQ_MEMBERS = 3;

  public static final long DEFAULT_GOSSIP_INTERVAL = 200;
  public static final int DEFAULT_GOSSIP_FANOUT = 3;
  public static final int DEFAULT_GOSSIP_REPEAT_MULTIPLIER = 2;

  private final List<Address> seedMembers;
  private final Map<String, String> metadata;
  private final int syncInterval;
  private final int syncTimeout;
  private final int suspectTimeout;
  private final String syncGroup;

  private final int pingInterval;
  private final int pingTimeout;
  private final int pingReqMembers;

  private final long gossipInterval;
  private final int gossipFanout;
  private final int gossipRepeatMultiplier;

  private final TransportConfig transportConfig;

  private ClusterConfig(Builder builder) {
    this.seedMembers = Collections.unmodifiableList(builder.seedMembers);
    this.metadata = Collections.unmodifiableMap(builder.metadata);
    this.syncInterval = builder.syncInterval;
    this.syncTimeout = builder.syncTimeout;
    this.suspectTimeout = builder.suspectTimeout;
    this.syncGroup = builder.syncGroup;

    this.pingInterval = builder.pingInterval;
    this.pingTimeout = builder.pingTimeout;
    this.pingReqMembers = builder.pingReqMembers;

    this.gossipFanout = builder.gossipFanout;
    this.gossipInterval = builder.gossipInterval;
    this.gossipRepeatMultiplier = builder.gossipRepeatMultiplier;

    this.transportConfig = builder.transportConfigBuilder.build();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static ClusterConfig defaultConfig() {
    return builder().build();
  }

  public List<Address> getSeedMembers() {
    return seedMembers;
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  public int getSyncInterval() {
    return syncInterval;
  }

  public int getSyncTimeout() {
    return syncTimeout;
  }

  public int getSuspectTimeout() {
    return suspectTimeout;
  }

  public String getSyncGroup() {
    return syncGroup;
  }

  public int getPingInterval() {
    return pingInterval;
  }

  public int getPingTimeout() {
    return pingTimeout;
  }

  public int getPingReqMembers() {
    return pingReqMembers;
  }

  public int getGossipFanout() {
    return gossipFanout;
  }

  public long getGossipInterval() {
    return gossipInterval;
  }

  public int getGossipRepeatMultiplier() {
    return gossipRepeatMultiplier;
  }

  public TransportConfig getTransportConfig() {
    return transportConfig;
  }

  @Override
  public String toString() {
    return "ClusterConfig{seedMembers=" + seedMembers
        + ", metadata=" + metadata
        + ", syncInterval=" + syncInterval
        + ", syncTimeout=" + syncTimeout
        + ", suspectTimeout=" + suspectTimeout
        + ", syncGroup='" + syncGroup + '\''
        + ", pingInterval=" + pingInterval
        + ", pingTimeout=" + pingTimeout
        + ", pingReqMembers=" + pingReqMembers
        + ", gossipInterval=" + gossipInterval
        + ", gossipFanout=" + gossipFanout
        + ", gossipRepeatMultiplier=" + gossipRepeatMultiplier
        + ", transportConfig=" + transportConfig
        + '}';
  }

  public static final class Builder {

    private List<Address> seedMembers = Collections.emptyList();
    private Map<String, String> metadata = Collections.emptyMap();
    private int syncInterval = DEFAULT_SYNC_INTERVAL;
    private int syncTimeout = DEFAULT_SYNC_TIMEOUT;
    private int suspectTimeout = DEFAULT_SUSPECT_TIMEOUT;
    private String syncGroup = DEFAULT_SYNC_GROUP;

    private int pingInterval = DEFAULT_PING_INTERVAL;
    private int pingTimeout = DEFAULT_PING_TIMEOUT;
    private int pingReqMembers = DEFAULT_PING_REQ_MEMBERS;

    private long gossipInterval = DEFAULT_GOSSIP_INTERVAL;
    private int gossipFanout = DEFAULT_GOSSIP_FANOUT;
    private int gossipRepeatMultiplier = DEFAULT_GOSSIP_REPEAT_MULTIPLIER;

    private TransportConfig.Builder transportConfigBuilder = TransportConfig.builder();

    private Builder() {}

    public Builder metadata(Map<String, String> metadata) {
      this.metadata = new HashMap<>(metadata);
      return this;
    }

    public Builder seedMembers(Address... seedMembers) {
      this.seedMembers = Arrays.asList(seedMembers);
      return this;
    }

    public Builder seedMembers(List<Address> seedMembers) {
      this.seedMembers = new ArrayList<>(seedMembers);
      return this;
    }

    public Builder syncInterval(int syncInterval) {
      this.syncInterval = syncInterval;
      return this;
    }

    public Builder syncTimeout(int syncTimeout) {
      this.syncTimeout = syncTimeout;
      return this;
    }

    public Builder suspectTimeout(int suspectTimeout) {
      this.suspectTimeout = suspectTimeout;
      return this;
    }

    public Builder syncGroup(String syncGroup) {
      this.syncGroup = syncGroup;
      return this;
    }

    public Builder pingInterval(int pingInterval) {
      this.pingInterval = pingInterval;
      return this;
    }

    public Builder pingTimeout(int pingTimeout) {
      this.pingTimeout = pingTimeout;
      return this;
    }

    public Builder pingReqMembers(int pingReqMembers) {
      this.pingReqMembers = pingReqMembers;
      return this;
    }

    public Builder gossipInterval(long gossipInterval) {
      this.gossipInterval = gossipInterval;
      return this;
    }

    public Builder gossipFanout(int gossipFanout) {
      this.gossipFanout = gossipFanout;
      return this;
    }

    public Builder gossipRepeatMultiplier(int gossipRepeatMultiplier) {
      this.gossipRepeatMultiplier = gossipRepeatMultiplier;
      return this;
    }

    /**
     * Sets all transport config settings equal to provided transport config.
     */
    public Builder transportConfig(TransportConfig transportConfig) {
      this.transportConfigBuilder.fillFrom(transportConfig);
      return this;
    }

    public Builder listenAddress(String listenAddress) {
      this.transportConfigBuilder.listenAddress(listenAddress);
      return this;
    }

    public Builder listenInterface(String listenInterface) {
      this.transportConfigBuilder.listenInterface(listenInterface);
      return this;
    }

    public Builder port(int port) {
      this.transportConfigBuilder.port(port);
      return this;
    }

    public Builder portAutoIncrement(boolean portAutoIncrement) {
      this.transportConfigBuilder.portAutoIncrement(portAutoIncrement);
      return this;
    }

    public Builder connectTimeout(int connectTimeout) {
      this.transportConfigBuilder.connectTimeout(connectTimeout);
      return this;
    }

    public Builder useNetworkEmulator(boolean useNetworkEmulator) {
      this.transportConfigBuilder.useNetworkEmulator(useNetworkEmulator);
      return this;
    }

    public ClusterConfig build() {
      Preconditions.checkState(pingTimeout < pingInterval, "Ping timeout can't be bigger than ping interval");
      return new ClusterConfig(this);
    }

  }

}
