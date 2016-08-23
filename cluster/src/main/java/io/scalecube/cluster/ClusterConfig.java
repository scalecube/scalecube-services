package io.scalecube.cluster;

import static com.google.common.base.Preconditions.checkNotNull;

import io.scalecube.cluster.fdetector.FailureDetectorConfig;
import io.scalecube.cluster.gossip.GossipConfig;
import io.scalecube.cluster.membership.MembershipConfig;
import io.scalecube.transport.TransportConfig;

import java.util.HashMap;
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

  public static final ClusterConfig DEFAULT = builder().build();

  public static final String DEFAULT_SEED_MEMBERS = "";
  public static final Map<String, String> DEFAULT_METADATA = new HashMap<>();

  private final String seedMembers;
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

  public String getSeedMembers() {
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

    private String seedMembers = DEFAULT_SEED_MEMBERS;
    private Map<String, String> metadata = DEFAULT_METADATA;

    private TransportConfig transportConfig = TransportConfig.DEFAULT;
    private MembershipConfig membershipConfig = MembershipConfig.DEFAULT;
    private FailureDetectorConfig failureDetectorConfig = FailureDetectorConfig.DEFAULT;
    private GossipConfig gossipConfig = GossipConfig.DEFAULT;

    private Builder() {}

    public Builder metadata(Map<String, String> metadata) {
      this.metadata = metadata;
      return this;
    }

    public Builder seedMembers(String seedMembers) {
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
