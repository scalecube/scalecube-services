package io.scalecube.examples;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.membership.MembershipRecord;
import io.scalecube.cluster.ICluster;
import io.scalecube.transport.Message;

import com.google.common.collect.ImmutableMap;

import rx.functions.Action1;
import rx.functions.Func1;

import java.util.Map;

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

  /**
   * Main method.
   */
  public static void main(String[] args) throws Exception {
    // Start seed cluster instance
    ICluster seedClusterInstance = Cluster.joinAwait();

    // Join Joe's cluster instance with metadata to seed cluster instance
    Map<String, String> metadata = ImmutableMap.of("alias", "Joe");
    ICluster joeClusterInstance = Cluster.joinAwait(metadata, seedClusterInstance.address());

    // Listen for messages to Joe on Joe's cluster insatnce and print them to system out
    joeClusterInstance.listen()
        .filter(new Func1<Message, Boolean>() {
          @Override
          public Boolean call(Message message) {
            return message.data() instanceof String && ((String) message.data()).contains("Joe");
          }
        }).subscribe(new Action1<Message>() {
          @Override
          public void call(Message message) {
            System.out.println(message.data());
          }
        });

    // Get the list of members in the cluster, locate Joe and send hello message
    for (MembershipRecord member : seedClusterInstance.otherMembers()) {
      if ("Joe".equals(member.metadata().get("alias"))) {
        seedClusterInstance.send(member, Message.fromData("Hello Joe"));
      }
    }
  }

}
