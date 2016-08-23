package io.scalecube.cluster;

import static com.google.common.base.Preconditions.checkNotNull;

import io.scalecube.cluster.fdetector.FailureDetectorConfig;
import io.scalecube.cluster.gossip.GossipConfig;
import io.scalecube.cluster.membership.MembershipConfig;
import io.scalecube.transport.Address;
import io.scalecube.transport.TransportConfig;

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
 *
 * @author Anton Kharenko
 */
public final class ClusterConfig {

  public static final List<Address> DEFAULT_SEED_MEMBERS = Collections.emptyList();
  public static final Map<String, String> DEFAULT_METADATA = new HashMap<>();
  private final List<Address> seedMembers;
  private final Map<String, String> metadata;
  private final TransportConfig transportConfig;
  private final MembershipConfig membershipConfig;
  private final FailureDetectorConfig failureDetectorConfig;
  private final GossipConfig gossipConfig;

  private ClusterConfig(Builder builder) {
    this.seedMembers = builder.seedMembers;
    this.metadata = builder.metadata;
    this.transportConfig = builder.transportConfig;
    this.membershipConfig = builder.membershipConfig;
    this.failureDetectorConfig = builder.failureDetectorConfig;
    this.gossipConfig = builder.gossipConfig;
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

  public TransportConfig getTransportConfig() {
    return transportConfig;
  }

  public MembershipConfig getMembershipConfig() {
    return membershipConfig;
  }

  public FailureDetectorConfig getFailureDetectorConfig() {
    return failureDetectorConfig;
  }

  public GossipConfig getGossipConfig() {
    return gossipConfig;
  }

  @Override
  public String toString() {
    return "ClusterConfig{seedMembers='" + seedMembers + '\''
        + ", metadata=" + metadata
        + ", transportConfig=" + transportConfig
        + ", membershipConfig=" + membershipConfig
        + ", failureDetectorConfig=" + failureDetectorConfig
        + ", gossipProtocolConfig=" + gossipConfig
        + '}';
  }

  public static final class Builder {

    private List<Address> seedMembers = DEFAULT_SEED_MEMBERS;
    private Map<String, String> metadata = DEFAULT_METADATA;

    private TransportConfig transportConfig = TransportConfig.defaultConfig();
    private MembershipConfig membershipConfig = MembershipConfig.defaultConfig();
    private FailureDetectorConfig failureDetectorConfig = FailureDetectorConfig.defaultConfig();
    private GossipConfig gossipConfig = GossipConfig.defaultConfig();

    private Builder() {}

    public Builder metadata(Map<String, String> metadata) {
      this.metadata = metadata;
      return this;
    }

    public Builder seedMembers(List<Address> seedMembers) {
      this.seedMembers = seedMembers;
      return this;
    }

    public Builder membershipConfig(MembershipConfig membershipConfig) {
      checkNotNull(membershipConfig);
      this.membershipConfig = membershipConfig;
      return this;
    }

    public Builder transportConfig(TransportConfig transportConfig) {
      checkNotNull(transportConfig);
      this.transportConfig = transportConfig;
      return this;
    }

    public Builder gossipConfig(GossipConfig gossipConfig) {
      checkNotNull(gossipConfig);
      this.gossipConfig = gossipConfig;
      return this;
    }

    public Builder failureDetectorConfig(FailureDetectorConfig failureDetectorConfig) {
      checkNotNull(failureDetectorConfig);
      this.failureDetectorConfig = failureDetectorConfig;
      return this;
    }

    public ClusterConfig build() {
      return new ClusterConfig(this);
    }

  }

}
