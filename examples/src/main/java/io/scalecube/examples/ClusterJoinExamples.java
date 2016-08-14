package io.scalecube.examples;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.ICluster;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Example how to create {@link ICluster} instance and use it.
 * 
 * @author Anton Kharenko
 */
public class ClusterJoinExamples {

  /**
   * Main method.
   */
  public static void main(String[] args) throws Exception {
    // Start seed member
    ICluster clusterNode1 = Cluster.joinAwait();

    // Define seed member address
    String seedMember = clusterNode1.localAddress().toString();

    // Join member to cluster
    ICluster clusterNode2 = Cluster.joinAwait(seedMember);

    // Start another member with metadata
    Map<String, String> metadata = ImmutableMap.of("alias", "another member");
    ClusterConfig config = ClusterConfig.newInstance().seedMembers(seedMember).metadata(metadata);
    ICluster clusterNode3 = Cluster.joinAwait(config);

    // Start cluster member in separate cluster (separate sync group)
    ClusterConfig.MembershipConfig membershipConfig = new ClusterConfig.MembershipConfig();
    membershipConfig.setSyncGroup("cluster-B");
    ClusterConfig config2 = ClusterConfig.newInstance().seedMembers(seedMember).membershipConfig(membershipConfig);
    ICluster anotherClusterNode = Cluster.joinAwait(config2);

    // Print first cluster members (3 nodes)
    System.out.println("Cluster 1: " + clusterNode1.membership().members());

    // Print second cluster members (single node)
    System.out.println("Cluster 2: " + anotherClusterNode.membership().members());

  }

}
