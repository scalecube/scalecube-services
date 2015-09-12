package io.servicefabric.cluster.gossip;

import static org.junit.Assert.assertEquals;

import io.servicefabric.transport.TransportEndpoint;
import io.servicefabric.transport.ITransport;
import io.servicefabric.transport.TransportHeaders;
import io.servicefabric.transport.TransportMessage;
import io.servicefabric.transport.protocol.Message;

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

  private GossipProtocol protocol;
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
        oneOf(executorService).scheduleWithFixedDelay(with(any(Runnable.class)), with(200l), with(200l),
            with(TimeUnit.MILLISECONDS));
      }
    });
    protocol = new GossipProtocol(TransportEndpoint.from("tcp://id@host:1"), executorService);
    protocol.setMaxGossipSent(maxGossipSent);
    protocol.setGossipTime(gossipTime);
    protocol.setMaxEndpointsToSelect(maxEndpointsToSelect);
    protocol.setTransport(transport);
    members = Lists.newArrayList();

    members.add(TransportEndpoint.from("tcp://id1@host:11"));
    members.add(TransportEndpoint.from("tcp://id2@host:22"));
    members.add(TransportEndpoint.from("tcp://id3@host:33"));

    protocol.setClusterEndpoints(this.members);
    protocol.start();
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
    protocol.listen().subscribe(new Action1<Message>() {
      @Override
      public void call(Message gossip) {
        res.add(gossip);
      }
    });
    List<Gossip> gossipList = new ArrayList<>();
    gossipList.add(new Gossip("1", new Message("data")));
    gossipList.add(new Gossip("2", new Message("data")));
    gossipList.add(new Gossip("3", new Message("data")));
    GossipRequest gossipRequest = new GossipRequest(gossipList);

    TransportEndpoint endpoint2 = TransportEndpoint.from("tcp://2@host:2");
    TransportEndpoint endpoint1 = TransportEndpoint.from("tcp://1@host:1");

    subject.onNext(new TransportMessage(new Message(gossipRequest), endpoint2));
    subject.onNext(new TransportMessage(new Message(null, TransportHeaders.QUALIFIER, "com.pt.openapi.hello/"), endpoint1));
    subject.onNext(new TransportMessage(new Message(gossipRequest), endpoint1));
    List<Gossip> second = new ArrayList<>();
    second.add(new Gossip("2", new Message("data")));
    second.add(new Gossip("4", new Message("data")));
    second.add(new Gossip("5", new Message("data")));
    subject.onNext(new TransportMessage(new Message(new GossipRequest(second)), endpoint1));
    Method processGossipQueueMethod = GossipProtocol.class.getDeclaredMethod("processGossipQueue");
    processGossipQueueMethod.setAccessible(true);
    processGossipQueueMethod.invoke(protocol);
    assertEquals(5, res.size());
  }

  @Test
  public void testSendGossips() throws Exception {
    Method sendGossips =
        GossipProtocol.class.getDeclaredMethod("sendGossips", List.class, Collection.class, Integer.class);
    sendGossips.setAccessible(true);
    List<GossipLocalState> list = new ArrayList<>();

    list.add(GossipLocalState.create(new Gossip("2", new Message("data")), TransportEndpoint.from("tcp://id2@host:22"), 0));
    jmockContext.checking(new Expectations() {
      {
        exactly(maxEndpointsToSelect).of(transport).send(with(any(TransportEndpoint.class)), with(any(Message.class)));
      }
    });
    sendGossips.invoke(protocol, members, list, 42);
  }
}
