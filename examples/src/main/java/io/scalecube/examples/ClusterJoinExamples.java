package io.scalecube.examples;

import static java.util.stream.Collectors.joining;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.Member;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Example how to create {@link Cluster} instances and join them to cluster.
 * 
 * @author Anton Kharenko
 */
public class ClusterJoinExamples {

  /**
   * Main method.
   */
  public static void main(String[] args) throws Exception {
    // Start seed member Alice
    Cluster alice = Cluster.joinAwait();

    // Join Bob to cluster with Alice
    Cluster bob = Cluster.joinAwait(alice.address());

    // Join Carol to cluster with metadata
    Map<String, String> metadata = ImmutableMap.of("name", "Carol");
    Cluster carol = Cluster.joinAwait(metadata, alice.address());

    // Start Dan on port 3000
    ClusterConfig configWithFixedPort = ClusterConfig.builder()
        .seedMembers(alice.address())
        .portAutoIncrement(false)
        .port(3000)
        .build();
    Cluster dan = Cluster.joinAwait(configWithFixedPort);

    // Start Eve in separate cluster (separate sync group)
    ClusterConfig configWithSyncGroup = ClusterConfig.builder()
        .seedMembers(alice.address(), bob.address(), carol.address(), dan.address()) // won't join anyway
        .syncGroup("another cluster")
        .build();
    Cluster eve = Cluster.joinAwait(configWithSyncGroup);

    // Print cluster members of each node

    System.out.println("Alice (" + alice.address() + ") cluster: "
        + alice.members().stream().map(Member::toString).collect(joining("\n", "\n", "\n")));

    System.out.println("Bob (" + bob.address() + ") cluster: "
        + bob.members().stream().map(Member::toString).collect(joining("\n", "\n", "\n")));

    System.out.println("Carol (" + carol.address() + ") cluster: "
        + carol.members().stream().map(Member::toString).collect(joining("\n", "\n", "\n")));

    System.out.println("Dan (" + dan.address() + ") cluster: "
        + dan.members().stream().map(Member::toString).collect(joining("\n", "\n", "\n")));

    System.out.println("Eve (" + eve.address() + ") cluster: " // alone in cluster
        + eve.members().stream().map(Member::toString).collect(joining("\n", "\n", "\n")));
  }

}
