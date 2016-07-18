package io.scalecube.examples;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.ICluster;

import java.util.HashMap;
import java.util.Map;

/**
 * Example how to create {@link ICluster} instance and use it.
 * 
 * @author Anton Kharenko
 */
public class ClusterBootstrapExamples {

  /**
   * Main method.
   */
  public static void main(String[] args) throws Exception {
    // Start seed members
    ICluster cluster1 = Cluster.joinAwait();
    ICluster cluster2 = Cluster.joinAwait(4001);

    String seedMembers = "localhost:" + ClusterConfig.DEFAULT_PORT + ", localhost:4001";

    // Start another member
    ICluster cluster3 = Cluster.joinAwait(4002, seedMembers);

    // Start cool member
    ICluster cluster4 = Cluster.joinAwait("Cool member", 4003, seedMembers);

    // Start another cool member with some metadata
    Map<String, String> metadata = new HashMap<>();
    metadata.put("key1", "value1");
    metadata.put("key2", "value2");
    ClusterConfig config5 =
        ClusterConfig.newInstance().port(4004).seedMembers(seedMembers).memberId("Another cool member")
            .metadata(metadata);
    ICluster cluster5 = Cluster.joinAwait(config5);

    // Alone cluster member - trying to join, but always ignored :(
    ClusterConfig.ClusterMembershipSettings membershipSettings7 =
        new ClusterConfig.ClusterMembershipSettings();
    membershipSettings7.setSyncGroup("forever alone");
    ClusterConfig config7 =
        ClusterConfig.newInstance().port(4006).seedMembers(seedMembers)
            .clusterMembershipSettings(membershipSettings7);
    ICluster cluster7 = Cluster.joinAwait(config7);
  }

}
