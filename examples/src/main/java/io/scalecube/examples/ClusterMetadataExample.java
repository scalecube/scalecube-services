package io.scalecube.examples;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.ICluster;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;

/**
 * Using Cluster metadata: metadata is set of custom parameters that may be used by application developers to attach
 * additional business information and identifications to cluster members.
 * 
 * <p>
 * in this example we see how to attach logical alias name to a cluster member we nick name Joe
 * </p>
 * 
 * @author ronen_h
 */
public class ClusterMetadataExample {

  public static void main(String[] args) throws Exception {
    // Start seed cluster instance
    ICluster seedClusterInstance = Cluster.joinAwait();
    Address seedAddress = seedClusterInstance.address();

    // Join cluster with Joe's cluster instance with metadata
    Map<String, String> metadata = ImmutableMap.of("alias", "Joe");
    ICluster joeClusterInstance = Cluster.joinAwait(metadata, seedAddress);

    // Listen for messages to Joe's cluster instance and print them to system out
    joeClusterInstance.listen()
        .map(Message::data)
        .subscribe(System.out::println);

    // Get the list of members in the cluster and find Joe
    Optional<Member> joeMemberOptional = seedClusterInstance.otherMembers().stream()
        .filter(member -> "Joe".equals(member.metadata().get("alias")))
        .findAny();
    // Send hello to Joe
    if (joeMemberOptional.isPresent()) {
      seedClusterInstance.send(joeMemberOptional.get(), Message.fromData("Hello Joe"));
    }
  }

}
