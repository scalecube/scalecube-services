package io.scalecube.examples;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.ICluster;
import io.scalecube.cluster.membership.MembershipConfig;
import io.scalecube.transport.Address;

import com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.Collections;
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

    // Join another member to cluster
    ICluster clusterNode2 = Cluster.joinAwait(clusterNode1.address());

    // Start another member with metadata
    Map<String, String> metadata = ImmutableMap.of("alias", "another member");
    ICluster clusterNode3 = Cluster.joinAwait(metadata, clusterNode1.address());

    // Start cluster member in separate cluster (separate sync group)
    ClusterConfig configWithSyncGroup = ClusterConfig.builder()
        .seedMembers(Collections.singletonList(clusterNode1.address()))
        .membershipConfig(MembershipConfig.builder().syncGroup("cluster-B").build())
        .build();
    ICluster anotherClusterNode = Cluster.joinAwait(configWithSyncGroup);

    // Print first cluster members (3 nodes)
    System.out.println("Cluster 1: " + clusterNode1.members());

    // Print second cluster members (single node)
    System.out.println("Cluster 2: " + anotherClusterNode.members());

  }

}
