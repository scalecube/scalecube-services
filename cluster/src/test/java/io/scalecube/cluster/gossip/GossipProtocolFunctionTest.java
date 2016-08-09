package io.scalecube.cluster.gossip;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.scalecube.transport.Message;
import io.scalecube.transport.Address;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class GossipProtocolFunctionTest {

  private Address remote;
  private Address local;

  @Before
  public void setup() {
    remote = Address.from("localhost:1");
    local = Address.from("localhost:2");
  }

  @Test
  public void testGossipMessageFilter() {
    GossipProtocol.GossipMessageFilter filter = new GossipProtocol.GossipMessageFilter();
    Message message = Message.fromData(new GossipRequest(Collections.<Gossip>emptyList()));
    assertTrue(filter.call(message));
    assertFalse(filter.call(Message.fromData("io.scalecube.hello/")));
  }

  @Test
  public void testOnGossipAction() {
    Queue<GossipProtocol.GossipTask> gossipQueue = new LinkedList<>();
    GossipProtocol.OnGossipRequestAction action = new GossipProtocol.OnGossipRequestAction(gossipQueue);
    List<Gossip> gossips = new ArrayList<>(20);
    for (int i = 0; i < 20; i++) {
      Gossip gossip = new Gossip("" + i, Message.fromData("123"));
      gossips.add(gossip);
    }
    Message message = Message.fromData(new GossipRequest(gossips));

    action.call(message);
    assertTrue(gossipQueue.size() == 20);
  }

  @Test
  public void testGossipDataToGossip() {
    Gossip gossip = new Gossip("1", Message.fromData("123"));
    GossipLocalState info = GossipLocalState.create(gossip, null, 0);
    GossipProtocol.GossipDataToGossipWithIncrement transform = new GossipProtocol.GossipDataToGossipWithIncrement();
    Gossip apply = transform.apply(info);
    Assert.assertEquals(gossip, apply);
    Assert.assertEquals(1, info.getSent());
  }

  @Test
  public void testGossipSendPredicate() {
    GossipProtocol.GossipSendPredicate predicate = new GossipProtocol.GossipSendPredicate(remote, 3);
    GossipLocalState info = GossipLocalState.create(new Gossip("1", Message.fromData(Collections.emptyMap())), local, 0);
    assertTrue(predicate.apply(info));
    info.addMember(remote);
    assertFalse(predicate.apply(info));
    GossipLocalState anotherInfo =
        GossipLocalState.create(new Gossip("2", Message.fromData(Collections.emptyMap())), local, 0);
    anotherInfo.incrementSend();
    anotherInfo.incrementSend();
    anotherInfo.incrementSend();
    assertFalse(predicate.apply(anotherInfo));

  }

  @Test
  public void testGossipSweepPredicate() {
    GossipProtocol.GossipSweepPredicate predicate = new GossipProtocol.GossipSweepPredicate(100, 10);
    GossipLocalState info = GossipLocalState.create(new Gossip("1", Message.fromData(Collections.emptyMap())), local, 50);
    assertTrue(predicate.apply(info));
    assertFalse(predicate
        .apply(GossipLocalState.create(new Gossip("1", Message.fromData(Collections.emptyMap())), local, 95)));
    assertFalse(predicate
        .apply(GossipLocalState.create(new Gossip("1", Message.fromData(Collections.emptyMap())), local, 90)));
  }
}
