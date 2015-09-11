package io.servicefabric.cluster.examples;

import io.servicefabric.cluster.Cluster;
import io.servicefabric.cluster.ClusterConfiguration;
import io.servicefabric.cluster.ICluster;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Anton Kharenko
 */
public class ClusterBootstrapExamples {

  public static void main(String[] args) throws InterruptedException {
    // Start seed members
    ICluster cluster1 = Cluster.newInstance().join();
    ICluster cluster2 = Cluster.newInstance(4001).join();

    String seedMembers = "localhost:" + ClusterConfiguration.DEFAULT_PORT + ", localhost:4001";

    // Start another member
    ICluster cluster3 = Cluster.newInstance(4002, seedMembers).join();

    // Start cool member
    ICluster cluster4 = Cluster.newInstance("Cool member", 4003, seedMembers).join();

    // Start another cool member with some metadata
    Map<String, String> metadata = new HashMap<>();
    metadata.put("key1", "value1");
    metadata.put("key2", "value2");
    ClusterConfiguration config5 =
        ClusterConfiguration.newInstance().port(4004).seedMembers(seedMembers).memberId("Another cool member")
            .metadata(metadata);
    ICluster cluster5 = Cluster.newInstance(config5).join();

    // If you don't want to call join
    ClusterConfiguration config6 =
        ClusterConfiguration.newInstance().port(4005).seedMembers(seedMembers).autoJoin(true);
    ICluster cluster6 = Cluster.newInstance(config6);

    // Alone cluster member - trying to join, but always ignored :(
    ClusterConfiguration.ClusterMembershipSettings membershipSettings7 =
        new ClusterConfiguration.ClusterMembershipSettings();
    membershipSettings7.setSyncGroup("forever alone");
    ClusterConfiguration config7 =
        ClusterConfiguration.newInstance().port(4006).seedMembers(seedMembers)
            .clusterMembershipSettings(membershipSettings7);
    ICluster cluster7 = Cluster.newInstance(config7).join();
  }

}
