package io.scalecube.stresstests.cluster;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.testlib.BaseTest;
import io.scalecube.transport.Message;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Anton Kharenko
 */
@RunWith(Parameterized.class)
public class ClusterMessagingStressTest extends BaseTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterMessagingStressTest.class);

  private static List<Object[]> experiments = Arrays.asList(new Object[][] {
      // Msg count
      {     1_000 }, // warm up
      {     1_000 },
      {     5_000 },
      {    10_000 },
      {    25_000 },
//      {    50_000 },
//      {   100_000 },
//      {   250_000 },
//      {   500_000 },
//      { 1_000_000 },
  });

  // Maximum time to await for all responses
  private static final int timeoutSeconds = 60;

  @Parameterized.Parameters(name = "msgCount={0}")
  public static List<Object[]> data() {
    return experiments;
  }

  private final int msgCount;

  public ClusterMessagingStressTest(int msgCount) {
    this.msgCount = msgCount;
  }

  @Test
  public void clusterMessagingStressTest() throws Exception {
    // Init transports
    Cluster echoServer = Cluster.joinAwait();
    Cluster[] latentNodes = new Cluster[ClusterConfig.DEFAULT_PING_REQ_MEMBERS];
    Cluster client1 = null;

    // Init measured params
    long sentTime = 0;
    long receivedTime = 0;
    LongSummaryStatistics rttStats = null;

    // Run experiment
    try {
      // Subscribe echo server handler
      echoServer.listen().subscribe(msg -> echoServer.send(msg.sender(), msg));

      // Start latent nodes (for indirect pings)
      for (int i = 0; i < latentNodes.length; i++) {
        latentNodes[i] = Cluster.joinAwait(echoServer.address());
      }

      // Init client
      CountDownLatch measureLatch = new CountDownLatch(msgCount);
      ArrayList<Long> rttRecords = new ArrayList<>(msgCount);
      client1 = Cluster.joinAwait(echoServer.address());
      client1.listen().subscribe(msg -> {
        long sentAt = Long.valueOf(msg.data());
        long rttTime = System.currentTimeMillis() - sentAt;
        rttRecords.add(rttTime);
        measureLatch.countDown();
      });

      // Subscribe on member removed event
      AtomicBoolean receivedMemberRemovedEvent = new AtomicBoolean(false);
      client1.listenMembership()
          .filter(MembershipEvent::isRemoved)
          .subscribe(event -> {
            LOGGER.warn("Received member removed event: {}", event);
            receivedMemberRemovedEvent.set(true);
          });

      // Measure
      long startAt = System.currentTimeMillis();
      for (int i = 0; i < msgCount; i++) {
        client1.send(echoServer.address(), Message.fromData(Long.toString(System.currentTimeMillis())));
      }
      sentTime = System.currentTimeMillis() - startAt;

      measureLatch.await(timeoutSeconds, TimeUnit.SECONDS);

      receivedTime = System.currentTimeMillis() - startAt;
      rttStats = rttRecords.stream().mapToLong(v -> v).summaryStatistics();
      assertTrue(measureLatch.getCount() == 0);
      assertFalse("Received member removed event", receivedMemberRemovedEvent.get());
    } finally {
      // Print results
      LOGGER.info("Finished sending {} messages in {} ms", msgCount, sentTime);
      LOGGER.info("Finished receiving {} messages in {} ms", msgCount, receivedTime);
      LOGGER.info("Round trip stats (ms): {}", rttStats);

      // Shutdown
      shutdown(echoServer);
      shutdown(latentNodes);
      shutdown(client1);
    }
  }

  private void shutdown(Cluster... nodes) {
    for (Cluster node : nodes) {
      try {
        node.shutdown().get();
      } catch (Exception ex) {
        LOGGER.error("Exception on cluster shutdown", ex);
      }
    }
  }

}
