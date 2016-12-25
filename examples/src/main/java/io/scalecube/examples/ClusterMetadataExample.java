package io.scalecube.examples;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.Member;
import io.scalecube.transport.Message;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Using Cluster metadata: metadata is set of custom parameters that may be used by application developers to attach
 * additional business information and identifications to cluster members.
 * 
 * <p>
 * in this example we see how to attach logical name to a cluster member we nick name Joe
 * </p>
 * 
 * @author ronen_h, Anton Kharenko
 */
public class ClusterMetadataExample {

  /**
   * Main method.
   */
  public static void main(String[] args) throws Exception {
    // Start seed cluster member Alice
    Cluster alice = Cluster.joinAwait();

    // Join Joe to cluster with metadata
    Map<String, String> metadata = ImmutableMap.of("name", "Joe");
    Cluster joe = Cluster.joinAwait(metadata, alice.address());

    // Subscribe Joe to listen for incoming messages and print them to system out
    joe.listen()
        .map(Message::data)
        .subscribe(System.out::println);

    // Scan the list of members in the cluster and find Joe there
    Optional<Member> joeMemberOptional = alice.otherMembers().stream()
        .filter(member -> "Joe".equals(member.metadata().get("name")))
        .findAny();

    // Send hello to Joe
    if (joeMemberOptional.isPresent()) {
      alice.send(joeMemberOptional.get(), Message.fromData("Hello Joe"));
    }

    TimeUnit.SECONDS.sleep(3);
  }

}
