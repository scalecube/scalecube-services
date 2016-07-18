package io.scalecube.examples;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.ClusterMember;
import io.scalecube.cluster.ICluster;
import io.scalecube.transport.Message;

import com.google.common.collect.ImmutableMap;

import rx.functions.Action1;
import rx.functions.Func1;

import java.util.List;

/**
 * Using Cluster metadata: metadata is set of custom paramters that may be used by application developers to attach
 * additional business information and identifications to cluster memebers.
 * 
 * <p>
 * in this example we see how to attach logical alias name to a cluster member we nick name Joe
 * </p>
 * 
 * @author ronen_h
 */
public class ClusterMetadataExample {

  private static final String MESSAGE_DATA = "hello/Joe";
  public static final Func1<Message, Boolean> MESSAGE_PREDICATE = new Func1<Message, Boolean>() {
    @Override
    public Boolean call(Message message) {
      return MESSAGE_DATA.equals(message.data());
    }
  };

  /**
   * Main method.
   */
  public static void main(String[] args) throws Exception {

    ICluster seedCluster = Cluster.joinAwait(3000);

    // define the custom configuration meta data. and we add alias field.
    ClusterConfig config = ClusterConfig.newInstance()
        .port(4004)
        .seedMembers("localhost:3000")
        .memberId("my_member_id")
        .metadata(ImmutableMap.of("alias", "Joe"));

    // configure cluster 2 with the metadata and attach cluster 2 as Joe and join seed
    ICluster joeCluster = Cluster.joinAwait(config);

    // filter and subscribe on hello/joe and print the welcome message.
    joeCluster.listen().filter(MESSAGE_PREDICATE).subscribe(new Action1<Message>() {
      @Override
      public void call(Message message) {
        System.out.println("Hello Joe");
      }
    });

    // get the list of members in the cluster and locate Joe tell Hello/Joe
    List<ClusterMember> members = seedCluster.membership().members();
    for (ClusterMember m : members) {
      if (m.metadata().containsKey("alias")) {
        if (m.metadata().get("alias").equals("Joe")) {
          seedCluster.send(m, new Message(MESSAGE_DATA));
        }
      }
    }
  }

}
