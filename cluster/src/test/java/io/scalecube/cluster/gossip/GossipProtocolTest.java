package io.scalecube.cluster.gossip;

import static org.junit.Assert.assertEquals;

import io.scalecube.transport.ITransport;
import io.scalecube.transport.Message;
import io.scalecube.transport.TransportEndpoint;

import com.google.common.collect.Lists;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import rx.functions.Action1;
import rx.subjects.PublishSubject;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class GossipProtocolTest {
  private static final int maxGossipSent = 2;
  private static final int gossipTime = 200;
  private static final int maxEndpointsToSelect = 2;

  private Mockery jmockContext;

  private GossipProtocol gossipProtocol;
  private PublishSubject subject;
  private ITransport transport;

  private ScheduledExecutorService executorService;
  private List<TransportEndpoint> members;

  @Before
  public void init() {
    jmockContext = new Mockery();
    subject = PublishSubject.create();
    transport = jmockContext.mock(ITransport.class);
    executorService = jmockContext.mock(ScheduledExecutorService.class);
    jmockContext.checking(new Expectations() {
      {
        oneOf(transport).listen();
        will(returnValue(subject));
        oneOf(executorService).scheduleWithFixedDelay(with(any(Runnable.class)), with(200L), with(200L),
            with(TimeUnit.MILLISECONDS));
      }
    });
    gossipProtocol = new GossipProtocol(TransportEndpoint.from("localhost:1:id"), executorService);
    gossipProtocol.setMaxGossipSent(maxGossipSent);
    gossipProtocol.setGossipTime(gossipTime);
    gossipProtocol.setMaxEndpointsToSelect(maxEndpointsToSelect);
    gossipProtocol.setTransport(transport);
    members = Lists.newArrayList();

    members.add(TransportEndpoint.from("localhost:1010:id1"));
    members.add(TransportEndpoint.from("localhost:2020:id2"));
    members.add(TransportEndpoint.from("localhost:3030:id3"));

    gossipProtocol.setClusterEndpoints(this.members);
    gossipProtocol.start();
  }

  @After
  public void destroy() {
    jmockContext.checking(new Expectations() {
      {
        oneOf(executorService).shutdownNow();
      }
    });
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testListenGossips() throws Exception {
    final List<Message> res = Lists.newArrayList();
    gossipProtocol.listen().subscribe(new Action1<Message>() {
      @Override
      public void call(Message gossip) {
        res.add(gossip);
      }
    });
    List<Gossip> gossipList = new ArrayList<>();
    gossipList.add(new Gossip("1", Message.fromData("data")));
    gossipList.add(new Gossip("2", Message.fromData("data")));
    gossipList.add(new Gossip("3", Message.fromData("data")));
    GossipRequest gossipRequest = new GossipRequest(gossipList);

    TransportEndpoint endpoint2 = TransportEndpoint.from("localhost:2:2");
    TransportEndpoint endpoint1 = TransportEndpoint.from("localhost:1:1");

    subject.onNext(messageWithSender(Message.fromData(gossipRequest), endpoint2));
    subject.onNext(messageWithSender(Message.fromQualifier("io.scalecube.hello/"), endpoint1));
    subject.onNext(messageWithSender(Message.fromData(gossipRequest), endpoint1));
    List<Gossip> second = new ArrayList<>();
    second.add(new Gossip("2", Message.fromData("data")));
    second.add(new Gossip("4", Message.fromData("data")));
    second.add(new Gossip("5", Message.fromData("data")));
    subject.onNext(messageWithSender(Message.fromData(new GossipRequest(second)), endpoint1));
    Method processGossipQueueMethod = GossipProtocol.class.getDeclaredMethod("processGossipQueue");
    processGossipQueueMethod.setAccessible(true);
    processGossipQueueMethod.invoke(gossipProtocol);
    assertEquals(5, res.size());
  }

  private Message messageWithSender(Message msg, TransportEndpoint sender) throws Exception {
    Method setSenderMethod = Message.class.getDeclaredMethod("setSender", TransportEndpoint.class);
    setSenderMethod.setAccessible(true);
    setSenderMethod.invoke(msg, sender);
    return msg;
  }

  @Test
  public void testSendGossips() throws Exception {
    Method sendGossips =
        GossipProtocol.class.getDeclaredMethod("sendGossips", List.class, Collection.class, Integer.class);
    sendGossips.setAccessible(true);
    List<GossipLocalState> list = new ArrayList<>();

    list.add(GossipLocalState.create(
        new Gossip("2", Message.fromData("data")), TransportEndpoint.from("localhost:2020:id2"), 0));
    jmockContext.checking(new Expectations() {
      {
        exactly(maxEndpointsToSelect).of(transport).send(with(any(TransportEndpoint.class)), with(any(Message.class)));
      }
    });
    sendGossips.invoke(gossipProtocol, members, list, 42);
  }
}
