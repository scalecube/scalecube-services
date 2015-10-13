package io.servicefabric.examples.metadata;

import io.servicefabric.cluster.Cluster;
import io.servicefabric.cluster.ClusterConfiguration;
import io.servicefabric.cluster.ClusterMember;
import io.servicefabric.cluster.ICluster;
import io.servicefabric.transport.Message;

import rx.functions.Action1;
import rx.functions.Func1;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Using Cluster metadata: metadata is set of custom paramters that may be used by application developers to attach
 * additional business information and identifications to cluster memebers.
 * 
 * <p>
 * in this example we see how to attach logical alias name to a cluster member we nick name Joe
 * </p>
 * @author ronen_h
 */
public class ClusterMetadata {

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

    ICluster seedCluster = Cluster.newInstance(3000).joinAwait();

    // define the custom configuration meta data. and we add alias field.
    Map<String, String> metadata = new HashMap<>();
    metadata.put("alias", "Joe");
    ClusterConfiguration config =
        ClusterConfiguration.newInstance().port(4004).seedMembers("localhost" + ":" + "3000").memberId("my_member_id")
            .metadata(metadata);

    // configure cluster 2 with the metadata and attach cluster 2 as Joe and join seed
    ICluster joeCluster = Cluster.newInstance(config).joinAwait();

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
