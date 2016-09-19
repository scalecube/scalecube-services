package io.scalecube.cluster.gossip;

import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.DummyMembershipProtocol;
import io.scalecube.cluster.membership.IMembershipProtocol;
import io.scalecube.transport.Message;
import io.scalecube.transport.Transport;
import io.scalecube.transport.Address;
import io.scalecube.transport.TransportConfig;

import com.google.common.util.concurrent.SettableFuture;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class GossipProtocolIT {

  @Test
  public void test10WithoutLostSmallDelay5Sec() throws Exception {
    int membersNum = 10;
    int lostPercent = 0;
    int meanDelay = 2;
    int timeout = 5000;
    testGossipProtocol(membersNum, lostPercent, meanDelay, timeout);
  }

  @Test
  public void test10Lost20SmallDelay5Sec() throws Exception {
    int membersNum = 10;
    int lostPercent = 20;
    int meanDelay = 2;
    int timeout = 5000;
    testGossipProtocol(membersNum, lostPercent, meanDelay, timeout);
  }

  @Test
  public void test50WithoutLostSmallDelay10Sec() throws Exception {
    int membersNum = 50;
    int lostPercent = 0;
    int meanDelay = 2;
    int timeout = 10000;
    testGossipProtocol(membersNum, lostPercent, meanDelay, timeout);
  }

  @Test
  public void test50Lost5BigDelay20Sec() throws Exception {
    int membersNum = 50;
    int lostPercent = 5;
    int meanDelay = 500;
    int timeout = 20000;
    testGossipProtocol(membersNum, lostPercent, meanDelay, timeout);
  }

  private void testGossipProtocol(int membersNum, int lostPercent, int delay, int timeout) throws Exception {
    // Init gossip protocol instances
    List<GossipProtocol> gossipProtocols = initGossipProtocols(membersNum, lostPercent, delay);

    // Subscribe on gossips
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
      long time = System.currentTimeMillis() - start;
      Assert.assertFalse("Delivered gossip twice to same member", doubleDelivery.get());
      Assert.assertEquals("Not all members received gossip", membersNum - 1, receivers.size());
      Assert.assertTrue("Time " + time + "ms is bigger then expected " + timeout + "ms", time < timeout);
      System.out.println("Time: " + time + "ms");
    } finally {
      // Destroy gossip protocol instances
      destroyGossipProtocols(gossipProtocols);
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
    // Await a bit
    try {
      Thread.sleep(1000);
    } catch (InterruptedException ignore) {
      // ignore
    }
    // Stop all transports
    for (GossipProtocol gossipProtocol : gossipProtocols) {
      SettableFuture<Void> close = SettableFuture.create();
      gossipProtocol.getTransport().stop(close);
      try {
        close.get(3, TimeUnit.SECONDS);
      } catch (Exception ignore) {
        System.out.println("Failed to await transport termination");
      }
    }
  }
}
