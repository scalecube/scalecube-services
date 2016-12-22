package io.scalecube.cluster.gossip;

import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.DummyMembershipProtocol;
import io.scalecube.cluster.membership.IMembershipProtocol;
import io.scalecube.transport.Message;
import io.scalecube.transport.Transport;
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

@RunWith(Parameterized.class)
public class GossipProtocolTest {

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
  private static final long gossipInterval /* ms */ = GossipConfig.DEFAULT_GOSSIP_INTERVAL;
  private static final int gossipFanout = GossipConfig.DEFAULT_GOSSIP_FANOUT;
  private static final int gossipFactor = GossipConfig.DEFAULT_GOSSIP_FACTOR;


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
    List<GossipProtocol> gossipProtocols = initGossipProtocols(membersNum, lossPercent, meanDelay);

    // Subscribe on gossips
    long disseminationTime = 0;
    long awaitCompletionTime = 0;
    LongSummaryStatistics messageSentStatsDissemination = null;
    LongSummaryStatistics messageLostStatsDissemination = null;
    LongSummaryStatistics messageSentStatsOverall = null;
    LongSummaryStatistics messageLostStatsOverall = null;
    long gossipLifetime = gossipProtocols.get(0).gossipLifetime();
    try {
      final String gossipData = "test gossip - " + ThreadLocalRandom.current().nextLong();
      final CountDownLatch latch = new CountDownLatch(membersNum - 1);
      final Map<Member, Member> receivers = new ConcurrentHashMap<>();
      final AtomicBoolean doubleDelivery = new AtomicBoolean(false);
      for (final GossipProtocol protocol : gossipProtocols) {
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
      latch.await(2 * gossipLifetime, TimeUnit.MILLISECONDS); // Await for double gossip lifetime
      disseminationTime = System.currentTimeMillis() - start;
      messageSentStatsDissemination = computeMessageSentStats(gossipProtocols);
      messageLostStatsDissemination = computeMessageLostStats(gossipProtocols);
      Assert.assertEquals("Not all members received gossip", membersNum - 1, receivers.size());
      Assert.assertTrue("Too long dissemination time " + disseminationTime
              + "ms (timeout " + gossipLifetime + "ms)", disseminationTime < gossipLifetime);

      // Await gossip lifetime plus few gossip intervals too ensure gossip is fully spread
      if (awaitFullCompletion) {
        awaitCompletionTime = gossipLifetime - disseminationTime + 3 * gossipInterval;
        Thread.sleep(awaitCompletionTime);

        messageSentStatsOverall = computeMessageSentStats(gossipProtocols);
        messageLostStatsOverall = computeMessageLostStats(gossipProtocols);
      }
      Assert.assertFalse("Delivered gossip twice to same member", doubleDelivery.get());
    } finally {
      // Print results
      LOGGER.info("[N={}, Ploss={}%, Tmean={}ms] Gossip dissemination time: {} ms (max {} ms)",
          membersNum, lossPercent, meanDelay, disseminationTime, gossipLifetime);
      LOGGER.info("[N={}, Ploss={}%, Tmean={}ms] Message sent until dissemination stats: {}",
          membersNum, lossPercent, meanDelay, messageSentStatsDissemination);
      LOGGER.info("[N={}, Ploss={}%, Tmean={}ms] Message lost until dissemination stats: {}",
          membersNum, lossPercent, meanDelay, messageLostStatsDissemination);
      if (awaitFullCompletion) {
        LOGGER.info("[N={}, Ploss={}%, Tmean={}ms] Message sent total stats (after await for {} ms): {}",
            membersNum, lossPercent, meanDelay, awaitCompletionTime, messageSentStatsOverall);
        LOGGER.info("[N={}, Ploss={}%, Tmean={}ms] Message lost total stats (after await for {} ms): {}",
            membersNum, lossPercent, meanDelay, awaitCompletionTime, messageLostStatsOverall);
      }

      // Destroy gossip protocol instances
      destroyGossipProtocols(gossipProtocols);

    }
  }

  private LongSummaryStatistics computeMessageSentStats(List<GossipProtocol> gossipProtocols) {
    List<Long> messageSentPerNode = new ArrayList<>(gossipProtocols.size());
    for (GossipProtocol gossipProtocol : gossipProtocols) {
      Transport transport = (Transport) gossipProtocol.getTransport();
      messageSentPerNode.add(transport.totalMessageSentCount());
    }
    return messageSentPerNode.stream().mapToLong(v -> v).summaryStatistics();
  }

  private LongSummaryStatistics computeMessageLostStats(List<GossipProtocol> gossipProtocols) {
    List<Long> messageLostPerNode = new ArrayList<>(gossipProtocols.size());
    for (GossipProtocol gossipProtocol : gossipProtocols) {
      Transport transport = (Transport) gossipProtocol.getTransport();
      messageLostPerNode.add(transport.totalMessageLostCount());
    }
    return messageLostPerNode.stream().mapToLong(v -> v).summaryStatistics();
  }

  private List<GossipProtocol> initGossipProtocols(int count, int lostPercent, int meanDelay) {
    final List<Transport> transports = initTransports(count, lostPercent, meanDelay);
    List<Address> members = new ArrayList<>();
    for (Transport transport : transports) {
      members.add(transport.address());
    }
    List<GossipProtocol> gossipProtocols = new ArrayList<>();
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
      transport.setDefaultNetworkSettings(lostPercent, meanDelay);
      transports.add(transport);
      startPort = transport.address().port() + 1;
    }
    return transports;
  }

  private GossipProtocol initGossipProtocol(Transport transport, List<Address> members) {
    IMembershipProtocol dummyMembership = new DummyMembershipProtocol(transport.address(), members);
    GossipConfig gossipConfig = GossipConfig.builder()
        .gossipFanout(gossipFanout)
        .gossipInterval(gossipInterval)
        .gossipFactor(gossipFactor)
        .build();
    GossipProtocol gossipProtocol = new GossipProtocol(transport, dummyMembership, gossipConfig);
    gossipProtocol.start();
    return gossipProtocol;
  }

  private void destroyGossipProtocols(List<GossipProtocol> gossipProtocols) {
    // Stop all gossip protocols
    for (GossipProtocol gossipProtocol : gossipProtocols) {
      gossipProtocol.stop();
    }

    // Stop all transports
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    for (GossipProtocol gossipProtocol : gossipProtocols) {
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
