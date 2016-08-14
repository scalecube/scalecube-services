package io.scalecube.cluster;

import io.scalecube.cluster.fdetector.FailureDetectorConfig;
import io.scalecube.cluster.gossip.GossipProtocolConfig;
import io.scalecube.cluster.membership.MembershipConfig;
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

  String seedMembers = "";
  Map<String, String> metadata = new HashMap<>();

  TransportConfig transportConfig = TransportConfig.DEFAULT;
  MembershipConfig membershipConfig = MembershipConfig.DEFAULT;
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

}
