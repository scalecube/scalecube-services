package io.scalecube.cluster.gossip;

import io.scalecube.transport.Message;
import io.scalecube.transport.Transport;
import io.scalecube.transport.TransportEndpoint;
import io.scalecube.transport.TransportSettings;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.SettableFuture;

import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import rx.functions.Action1;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class GossipEmulationIT {

  private static final int TIMEOUT = 20050;

  private List<GossipProtocol> gossipers;

  @After
  public void destroy() throws Exception {
    for (GossipProtocol gossiper : gossipers) {
      gossiper.stop();
      SettableFuture<Void> close = SettableFuture.create();
      gossiper.getTransport().stop(close);
      try {
        close.get(1, TimeUnit.SECONDS);
      } catch (Exception ignore) {
      }
    }
  }

  @Test
  public void test10WithoutLostSmallDelay() throws Exception {
    int membersNum = 10;
    int lostPercent = 0;
    int delay = 2;
    gossipers = initGossipers(membersNum, lostPercent, delay);

    final CountDownLatch latch = new CountDownLatch(membersNum - 1);
    for (final GossipProtocol protocol : gossipers) {
      protocol.listen().subscribe(new Action1<Message>() {
        @Override
        public void call(Message gossip) {
          latch.countDown();
        }
      });
    }

    long start = System.currentTimeMillis();
    gossipers.get(0).spread(Message.fromData("data"));
    latch.await(20, TimeUnit.SECONDS);
    long time = System.currentTimeMillis() - start;
    Assert.assertTrue("Time: " + time, time < TIMEOUT);
    System.out.println("Time: " + time);
  }

  @Test
  public void test10Lost20SmallDelay() throws Exception {
    int membersNum = 10;
    int lostPercent = 20;
    int delay = 2;
    gossipers = initGossipers(membersNum, lostPercent, delay);

    final CountDownLatch latch = new CountDownLatch(membersNum - 1);
    for (final GossipProtocol protocol : gossipers) {
      protocol.listen().subscribe(new Action1<Message>() {
        @Override
        public void call(Message gossip) {
          latch.countDown();
        }
      });
    }

    long start = System.currentTimeMillis();
    gossipers.get(0).spread(Message.fromData("data"));
    latch.await(20, TimeUnit.SECONDS);
    long time = System.currentTimeMillis() - start;
    Assert.assertTrue("Time: " + time, time < TIMEOUT);
    System.out.println("Time: " + time);
  }

  @Test
  public void test100WithoutLostSmallDelay() throws Exception {
    int membersNum = 100;
    int lostPercent = 0;
    int delay = 2;
    gossipers = initGossipers(membersNum, lostPercent, delay);

    final CountDownLatch latch = new CountDownLatch(membersNum - 1);
    for (final GossipProtocol protocol : gossipers) {
      protocol.listen().subscribe(new Action1<Message>() {
        @Override
        public void call(Message gossip) {
          latch.countDown();
        }
      });
    }

    long start = System.currentTimeMillis();
    gossipers.get(0).spread(Message.fromData("data"));
    latch.await(20, TimeUnit.SECONDS);
    long time = System.currentTimeMillis() - start;
    Assert.assertTrue("Time: " + time, time < TIMEOUT);
    System.out.println("Time: " + time);
  }

  @Test
  public void test100Lost5BigDelay() throws Exception {
    int membersNum = 100;
    int lostPercent = 5;
    int delay = 500;
    gossipers = initGossipers(membersNum, lostPercent, delay);

    final CountDownLatch latch = new CountDownLatch(membersNum - 1);
    for (final GossipProtocol protocol : gossipers) {
      protocol.listen().subscribe(new Action1<Message>() {
        @Override
        public void call(Message gossip) {
          latch.countDown();
        }
      });
    }

    long start = System.currentTimeMillis();
    gossipers.get(0).spread(Message.fromData("data"));
    latch.await(20, TimeUnit.SECONDS);
    long time = System.currentTimeMillis() - start;
    Assert.assertTrue("Time: " + time, time < TIMEOUT);
    System.out.println("Time: " + time);
  }

  @Ignore
  @Test
  public void test1000Lost10BigDelay() throws Exception {
    int membersNum = 1000;
    int lostPercent = 10;
    int delay = 10000;
    gossipers = initGossipers(membersNum, lostPercent, delay);

    final CountDownLatch latch = new CountDownLatch(membersNum - 1);
    for (final GossipProtocol protocol : gossipers) {
      protocol.listen().subscribe(new Action1<Message>() {
        @Override
        public void call(Message gossip) {
          latch.countDown();
        }
      });
    }

    long start = System.currentTimeMillis();
    gossipers.get(0).spread(Message.fromData("data"));
    latch.await(20, TimeUnit.SECONDS);
    long time = System.currentTimeMillis() - start;
    Assert.assertTrue("Time: " + time, time < TIMEOUT);
    System.out.println("Time: " + time);
  }

  @Ignore
  @Test
  public void test10000Lost5SmallDelay() throws Exception {
    int membersNum = 10000;
    int lostPercent = 5;
    int delay = 2;
    gossipers = initGossipers(membersNum, lostPercent, delay);

    final CountDownLatch latch = new CountDownLatch(membersNum - 1);
    for (final GossipProtocol protocol : gossipers) {
      protocol.listen().subscribe(new Action1<Message>() {
        @Override
        public void call(Message gossip) {
          latch.countDown();
        }
      });
    }

    long start = System.currentTimeMillis();
    gossipers.get(0).spread(Message.fromData("data"));
    latch.await(30, TimeUnit.SECONDS);
    long time = System.currentTimeMillis() - start;
    Assert.assertTrue("Time: " + time + "count: " + latch.getCount(), time < 30000);
    System.out.println("Time: " + time);
  }

  @Ignore
  @Test
  public void test1000WithoutLostSmallDelay() throws Exception {
    int membersNum = 1000;
    int lostPercent = 0;
    int delay = 2;
    gossipers = initGossipers(membersNum, lostPercent, delay);

    final CountDownLatch latch = new CountDownLatch(membersNum - 1);
    for (final GossipProtocol protocol : gossipers) {
      protocol.listen().subscribe(new Action1<Message>() {
        @Override
        public void call(Message gossip) {
          latch.countDown();
        }
      });
    }

    long start = System.currentTimeMillis();
    gossipers.get(0).spread(Message.fromData("data"));
    latch.await(20, TimeUnit.SECONDS);
    long time = System.currentTimeMillis() - start;
    Assert.assertTrue("Time: " + time, time < TIMEOUT);
    System.out.println("Time: " + time);
  }

  private List<GossipProtocol> initGossipers(int membersNum, int lostPercent, int delay) {
    final List<TransportEndpoint> members = initMembers(membersNum);
    gossipers = Lists.newArrayList();
    for (TransportEndpoint member : members) {
      gossipers.add(initGossiper(member, members, lostPercent, delay));
    }
    return gossipers;
  }

  private List<TransportEndpoint> initMembers(int membersNum) {
    List<TransportEndpoint> members = new ArrayList<>(membersNum);
    for (int id = 0; id < membersNum; id++) {
      int port = 20000 + id;
      members.add(TransportEndpoint.from("localhost:" + port + ":" + id));
    }
    return members;
  }

  private GossipProtocol initGossiper(TransportEndpoint transportEndpoint, List<TransportEndpoint> members,
      int lostPercent, int delay) {

    TransportSettings transportSettings = TransportSettings.builder().useNetworkEmulator(true).build();
    Transport transport = Transport.newInstance(transportEndpoint, transportSettings);
    transport.setDefaultNetworkSettings(lostPercent, delay);

    GossipProtocol gossipProtocol = new GossipProtocol(transport);
    gossipProtocol.setClusterEndpoints(members);

    try {
      transport.start().get();
    } catch (Exception ex) {
      Throwables.propagate(ex);
    }
    gossipProtocol.start();

    return gossipProtocol;
  }
}
