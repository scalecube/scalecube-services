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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

@RunWith(Parameterized.class)
public class GossipProtocolTest {

  @Parameterized.Parameters(name = "N={0}, Plost={1}%, Tmean={2}ms, T={3}ms")
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][] {
        { 2  , 0  ,  2   , 1_000  }, // warm up
        { 2  , 0  ,  2   ,   500  },
        { 3  , 0  ,  2   ,   500  },
        { 5  , 0  ,  2   ,   800  },
        { 10 , 0  ,  2   , 1_000  },
        { 10 , 25 ,  2   , 2_000  },
        { 10 , 50 ,  2   , 4_000  },
        { 50 , 0  ,  2   , 5_000  },
        { 50 , 10  , 300 , 10_000 },
    });
  }

  private final int membersNum;
  private final int lostPercent;
  private final int meanDelay;
  private final int timeout;

  public GossipProtocolTest(Integer membersNum, Integer lostPercent, Integer meanDelay, Integer timeout) {
    this.membersNum = membersNum;
    this.lostPercent = lostPercent;
    this.meanDelay = meanDelay;
    this.timeout = timeout;
  }

  @Test
  public void testGossipProtocol() throws Exception {
    // Init gossip protocol instances
    List<GossipProtocol> gossipProtocols = initGossipProtocols(membersNum, lostPercent, meanDelay);

    // Subscribe on gossips
    long time = 0;
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
              System.out.println("Delivered gossip twice to: " + protocol.getTransport().address());
              doubleDelivery.set(true);
            }
          }
        });
      }

      // Spread gossip, measure and verify delivery metrics
      long start = System.currentTimeMillis();
      gossipProtocols.get(0).spread(Message.fromData(gossipData));
      latch.await(2 * timeout, TimeUnit.MILLISECONDS); // Await double timeout
      time = System.currentTimeMillis() - start;
      Assert.assertFalse("Delivered gossip twice to same member", doubleDelivery.get());
      Assert.assertEquals("Not all members received gossip", membersNum - 1, receivers.size());
      Assert.assertTrue("Time " + time + "ms is bigger then expected " + timeout + "ms", time < timeout);
    } finally {
      // Destroy gossip protocol instances
      destroyGossipProtocols(gossipProtocols);
      System.out.println("Gossip dissemination time: " + time + " ms");
    }
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
    GossipProtocol gossipProtocol = new GossipProtocol(transport, dummyMembership, GossipConfig.defaultConfig());
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
      System.out.println("Failed to await transport termination");
    }

    // Await a bit
    try {
      Thread.sleep(gossipProtocols.size() * 20);
    } catch (InterruptedException ignore) {
      // ignore
    }
  }
}
