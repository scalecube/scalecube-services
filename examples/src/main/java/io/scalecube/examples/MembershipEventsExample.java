package io.scalecube.examples;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ICluster;
import io.scalecube.cluster.fdetector.FailureDetectorConfig;
import io.scalecube.cluster.membership.MembershipConfig;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Example of subscribing and listening for cluster membership events which is emmited when new member joins or leave
 * cluster.
 *
 * @author Anton Kharenko
 */
public class MembershipEventsExample {

  private static final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");

  /**
   * Main method.
   */
  public static void main(String[] args) throws Exception {
    // Alice init cluster
    ICluster alice = Cluster.joinAwait(ImmutableMap.of("name", "Alice"));
    System.out.println(now() + " Alice join members: " + alice.members());
    alice.listenMembership()
        .subscribe(event -> System.out.println(now() + " Alice received: " + event));

    // Bob join cluster
    ICluster bob = Cluster.joinAwait(ImmutableMap.of("name", "Bob"), alice.address());
    System.out.println(now() + " Bob join members: " + bob.members());
    bob.listenMembership()
        .subscribe(event -> System.out.println(now() + " Bob received: " + event));

    // Carol join cluster
    ICluster carol = Cluster.joinAwait(ImmutableMap.of("name", "Carol"), alice.address(), bob.address());
    System.out.println(now() + " Carol join members: " + carol.members());
    carol.listenMembership()
        .subscribe(event -> System.out.println(now() + " Carol received: " + event));

    // Bob leave cluster
    ListenableFuture<Void> shutdownFuture = bob.shutdown();
    shutdownFuture.get();

    // Avoid exit main thread immediately ]:->
    long maxRemoveTimeout = MembershipConfig.DEFAULT_SUSPECT_TIMEOUT + 3 * FailureDetectorConfig.DEFAULT_PING_INTERVAL;
    Thread.sleep(maxRemoveTimeout);
  }

  private static String now() {
    return sdf.format(new Date());
  }

}
