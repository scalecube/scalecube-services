package io.scalecube.cluster.gossip;

import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.ClusterMath;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.DummyMembershipProtocol;
import io.scalecube.cluster.membership.MembershipProtocol;
import io.scalecube.testlib.BaseTest;
import io.scalecube.transport.Transport;
import io.scalecube.transport.Message;
import io.scalecube.transport.Address;
import io.scalecube.transport.TransportConfig;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.scalecube.cluster.ClusterMath.gossipConvergencePercent;
import static io.scalecube.cluster.ClusterMath.gossipDisseminationTime;
import static io.scalecube.cluster.ClusterMath.maxMessagesPerGossipPerNode;
import static io.scalecube.cluster.ClusterMath.maxMessagesPerGossipTotal;

@RunWith(Parameterized.class)
public class GossipProtocolTest extends BaseTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(GossipProtocolTest.class);

  private static List<Object[]> experiments = Arrays.asList(new Object[][] {
//      N  , L  ,  D      // N - num of nodes, L - msg loss percent, D - msg mean delay (ms)
      { 2  , 0  ,  2   }, // warm up
      { 2  , 0  ,  2   },
      { 3  , 0  ,  2   },
      { 5  , 0  ,  2   },
      { 10 , 0  ,  2   },
      { 10 , 10 ,  2   },
      { 10 , 25 ,  2   },
      { 10 , 25 ,  100 },
      { 10 , 50 ,  2   },
      { 50 , 0  ,  2   },
      { 50 , 10 ,  2   },
      { 50 , 10 ,  100 },
  });

  // Makes tests run longer since always awaits for maximum gossip lifetime, but performs more checks
  private static final boolean awaitFullCompletion = true;

  // Allow to configure gossip settings other than defaults
  private static final long gossipInterval /* ms */ = ClusterConfig.DEFAULT_GOSSIP_INTERVAL;
  private static final int gossipFanout = ClusterConfig.DEFAULT_GOSSIP_FANOUT;
  private static final int gossipRepeatMultiplier = ClusterConfig.DEFAULT_GOSSIP_REPEAT_MULT;


  // Uncomment and modify params to run single experiment repeatedly
//  static {
//    int repeatCount = 1000;
//    int membersNum = 10;
//    int lossPercent = 50; //%
//    int meanDelay = 2; //ms
//    experiments = new ArrayList<>(repeatCount + 1);
//    experiments.add(new Object[] {2, 0, 2}); // add warm up experiment
//    for (int i = 0; i < repeatCount; i++) {
//      experiments.add(new Object[] {membersNum, lossPercent,  meanDelay});
//    }
//  }

  @Parameterized.Parameters(name = "N={0}, Ploss={1}%, Tmean={2}ms")
  public static List<Object[]> data() {
    return experiments;
  }

  private final int membersNum;
  private final int lossPercent;
  private final int meanDelay;

  public GossipProtocolTest(int membersNum, int lossPercent, int meanDelay) {
    this.membersNum = membersNum;
    this.lossPercent = lossPercent;
    this.meanDelay = meanDelay;
  }

  @Test
  public void testGossipProtocol() throws Exception {
    // Init gossip protocol instances
    List<GossipProtocolImpl> gossipProtocols = initGossipProtocols(membersNum, lossPercent, meanDelay);

    // Subscribe on gossips
    long disseminationTime = 0;
    LongSummaryStatistics messageSentStatsDissemination = null;
    LongSummaryStatistics messageLostStatsDissemination = null;
    LongSummaryStatistics messageSentStatsOverall = null;
    LongSummaryStatistics messageLostStatsOverall = null;
    long gossipTimeout = ClusterMath.gossipTimeoutToSweep(gossipRepeatMultiplier, membersNum, gossipInterval);
    try {
      final String gossipData = "test gossip - " + ThreadLocalRandom.current().nextLong();
      final CountDownLatch latch = new CountDownLatch(membersNum - 1);
      final Map<Member, Member> receivers = new ConcurrentHashMap<>();
      final AtomicBoolean doubleDelivery = new AtomicBoolean(false);
      for (final GossipProtocolImpl protocol : gossipProtocols) {
        protocol.listen().subscribe(gossip -> {
          if (gossipData.equals(gossip.data())) {
            boolean firstTimeAdded = receivers.put(protocol.getMember(), protocol.getMember()) == null;
            if (firstTimeAdded) {
              latch.countDown();
            } else {
              LOGGER.error("Delivered gossip twice to: {}", protocol.getTransport().address());
              doubleDelivery.set(true);
            }
          }
        });
      }

      // Spread gossip, measure and verify delivery metrics
      long start = System.currentTimeMillis();
      gossipProtocols.get(0).spread(Message.fromData(gossipData));
      latch.await(2 * gossipTimeout, TimeUnit.MILLISECONDS); // Await for double gossip timeout
      disseminationTime = System.currentTimeMillis() - start;
      messageSentStatsDissemination = computeMessageSentStats(gossipProtocols);
      if (lossPercent > 0) {
        messageLostStatsDissemination = computeMessageLostStats(gossipProtocols);
      }
      Assert.assertEquals("Not all members received gossip", membersNum - 1, receivers.size());
      Assert.assertTrue("Too long dissemination time " + disseminationTime
              + "ms (timeout " + gossipTimeout + "ms)", disseminationTime < gossipTimeout);

      // Await gossip lifetime plus few gossip intervals too ensure gossip is fully spread
      if (awaitFullCompletion) {
        long awaitCompletionTime = gossipTimeout - disseminationTime + 3 * gossipInterval;
        Thread.sleep(awaitCompletionTime);

        messageSentStatsOverall = computeMessageSentStats(gossipProtocols);
        if (lossPercent > 0) {
          messageLostStatsOverall = computeMessageLostStats(gossipProtocols);
        }
      }
      Assert.assertFalse("Delivered gossip twice to same member", doubleDelivery.get());
    } finally {
      // Print theoretical results
      LOGGER.info("Experiment params: N={}, Gfanout={}, Grepeat_mult={}, Tgossip={}ms Ploss={}%, Tmean={}ms",
          membersNum, gossipFanout, gossipRepeatMultiplier, gossipInterval, lossPercent, meanDelay);
      double convergProb = gossipConvergencePercent(gossipFanout, gossipRepeatMultiplier, membersNum, lossPercent);
      long expDissemTime = gossipDisseminationTime(gossipRepeatMultiplier, membersNum, gossipInterval);
      int maxMsgPerNode = maxMessagesPerGossipPerNode(gossipFanout, gossipRepeatMultiplier, membersNum);
      int maxMsgTotal = maxMessagesPerGossipTotal(gossipFanout, gossipRepeatMultiplier, membersNum);
      LOGGER.info("Expected dissemination time is {}ms with probability {}%", expDissemTime, convergProb);
      LOGGER.info("Max messages sent per node {} and total {}", maxMsgPerNode, maxMsgTotal);

      // Print actual results
      LOGGER.info("Actual dissemination time: {}ms (timeout {}ms)", disseminationTime, gossipTimeout);
      LOGGER.info("Messages sent stats (diss.): {}", messageSentStatsDissemination);
      if (lossPercent > 0) {
        LOGGER.info("Messages lost stats (diss.): {}", messageLostStatsDissemination);
      }
      if (awaitFullCompletion) {
        LOGGER.info("Messages sent stats (total): {}", messageSentStatsOverall);
        if (lossPercent > 0) {
          LOGGER.info("Messages lost stats (total): {}", messageLostStatsOverall);
        }
      }

      // Destroy gossip protocol instances
      destroyGossipProtocols(gossipProtocols);

    }
  }

  private LongSummaryStatistics computeMessageSentStats(List<GossipProtocolImpl> gossipProtocols) {
    List<Long> messageSentPerNode = new ArrayList<>(gossipProtocols.size());
    for (GossipProtocolImpl gossipProtocol : gossipProtocols) {
      Transport transport = gossipProtocol.getTransport();
      messageSentPerNode.add(transport.networkEmulator().totalMessageSentCount());
    }
    return messageSentPerNode.stream().mapToLong(v -> v).summaryStatistics();
  }

  private LongSummaryStatistics computeMessageLostStats(List<GossipProtocolImpl> gossipProtocols) {
    List<Long> messageLostPerNode = new ArrayList<>(gossipProtocols.size());
    for (GossipProtocolImpl gossipProtocol : gossipProtocols) {
      Transport transport = gossipProtocol.getTransport();
      messageLostPerNode.add(transport.networkEmulator().totalMessageLostCount());
    }
    return messageLostPerNode.stream().mapToLong(v -> v).summaryStatistics();
  }

  private List<GossipProtocolImpl> initGossipProtocols(int count, int lostPercent, int meanDelay) {
    final List<Transport> transports = initTransports(count, lostPercent, meanDelay);
    List<Address> members = new ArrayList<>();
    for (Transport transport : transports) {
      members.add(transport.address());
    }
    List<GossipProtocolImpl> gossipProtocols = new ArrayList<>();
    for (Transport transport : transports) {
      gossipProtocols.add(initGossipProtocol(transport, members));
    }
    return gossipProtocols;
  }

  private List<Transport> initTransports(int count, int lostPercent, int meanDelay) {
    List<Transport> transports = new ArrayList<>(count);
    int startPort = TransportConfig.DEFAULT_PORT;
    for (int i = 0; i < count; i++) {
      TransportConfig transportConfig = TransportConfig.builder()
          .useNetworkEmulator(true)
          .port(startPort)
          .portCount(1000)
          .build();
      Transport transport = Transport.bindAwait(transportConfig);
      transport.networkEmulator().setDefaultLinkSettings(lostPercent, meanDelay);
      transports.add(transport);
      startPort = transport.address().port() + 1;
    }
    return transports;
  }

  private GossipProtocolImpl initGossipProtocol(Transport transport, List<Address> members) {
    MembershipProtocol dummyMembership = new DummyMembershipProtocol(transport.address(), members);
    GossipConfig gossipConfig = ClusterConfig.builder()
        .gossipFanout(gossipFanout)
        .gossipInterval(gossipInterval)
        .gossipRepeatMult(gossipRepeatMultiplier)
        .build();
    GossipProtocolImpl gossipProtocol = new GossipProtocolImpl(transport, dummyMembership, gossipConfig);
    gossipProtocol.start();
    return gossipProtocol;
  }

  private void destroyGossipProtocols(List<GossipProtocolImpl> gossipProtocols) {
    // Stop all gossip protocols
    for (GossipProtocolImpl gossipProtocol : gossipProtocols) {
      gossipProtocol.stop();
    }

    // Stop all transports
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    for (GossipProtocolImpl gossipProtocol : gossipProtocols) {
      CompletableFuture<Void> close = new CompletableFuture<>();
      gossipProtocol.getTransport().stop(close);
      futures.add(close);
    }
    try {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get(30, TimeUnit.SECONDS);
    } catch (Exception ignore) {
      LOGGER.warn("Failed to await transport termination");
    }

    // Await a bit
    try {
      Thread.sleep(gossipProtocols.size() * 20);
    } catch (InterruptedException ignore) {
      // ignore
    }
  }
}
