package io.scalecube.ipc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.scalecube.ipc.Event.Topic;
import io.scalecube.transport.Address;

import org.junit.Test;

import rx.subjects.BehaviorSubject;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class DefaultEventStreamTest {

  private ServiceMessage message0 = ServiceMessage.withQualifier("ok").build();
  private ServiceMessage message1 = ServiceMessage.withQualifier("hola").build();

  private ChannelContext ctx0 = ChannelContext.create(Address.from("localhost:0"));
  private ChannelContext ctx1 = ChannelContext.create(Address.from("localhost:1"));

  private DefaultEventStream eventStream = new DefaultEventStream();

  @Test
  public void testChannelContextPostEvent() {
    List<Event> events = new ArrayList<>(2);
    eventStream.listen().subscribe(events::add);
    eventStream.subscribe(ctx0);
    eventStream.subscribe(ctx1);

    ctx0.postReadSuccess(message0);
    ctx0.postReadSuccess(message0);
    ctx1.postReadSuccess(message1);
    ctx1.postReadSuccess(message1);

    assertEquals(message0, events.get(0).getMessage().get());
    assertEquals(message0, events.get(1).getMessage().get());
    assertEquals(message1, events.get(2).getMessage().get());
    assertEquals(message1, events.get(3).getMessage().get());
  }

  @Test
  public void testChannelContextsAreIsolated() {
    BehaviorSubject<Event> subject = BehaviorSubject.create();
    eventStream.listen().subscribe(subject);
    eventStream.subscribe(ctx0);
    eventStream.subscribe(ctx1);

    ctx0.postReadSuccess(message0);
    assertEquals(message0, subject.test().getOnNextEvents().get(0).getMessageOrThrow());

    // at this point close ctx1
    ctx1.close();

    // keep posting via ctx0
    ctx0.postReadSuccess(message1);
    assertEquals(message1, subject.test().getOnNextEvents().get(0).getMessageOrThrow());
  }

  @Test
  public void testUnsubscribingChannelContextIsIsolated() {
    BehaviorSubject<Event> subject = BehaviorSubject.create();
    eventStream.listen().subscribe(subject);
    eventStream.subscribe(ctx0);
    eventStream.subscribe(ctx1);

    ctx0.postReadSuccess(message0);
    assertEquals(message0, subject.test().getOnNextEvents().get(0).getMessageOrThrow());

    ctx1.postReadSuccess(message1);
    assertEquals(message1, subject.test().getOnNextEvents().get(0).getMessageOrThrow());

    ctx0.close();
    ctx1.close();

    // After two contexts closed business layer is not affected
    ChannelContext ctx2 = ChannelContext.create(Address.from("localhost:2"));
    eventStream.subscribe(ctx2);
    ctx2.postReadError(new RuntimeException("Can't decode incoming msg"));
    assertEquals(Topic.ReadError, subject.test().getOnNextEvents().get(0).getTopic());
  }

  @Test
  public void testChannelContextClosed() {
    AtomicBoolean eventSubjectClosed = new AtomicBoolean();
    AtomicBoolean channelContextClosed = new AtomicBoolean();
    // You can watch-out for close at major Observable<Event>
    ctx0.listen().subscribe(event -> {
    }, throwable -> {
    }, () -> eventSubjectClosed.set(true));
    // You can watch-out for close at Observable that was invented for exact reason
    ctx0.listenClose(ctx -> channelContextClosed.set(true));
    ctx0.close();
    assertTrue(eventSubjectClosed.get());
    assertTrue(channelContextClosed.get());
  }

  @Test
  public void testChannelContextClosedCheckItsState() {
    AtomicBoolean channelContextCompleted = new AtomicBoolean();
    ChannelContext[] channelContexts = new ChannelContext[1];
    ctx0.listenClose(ctx -> {
      channelContexts[0] = ctx;
      // try listen
      ctx.listen().subscribe(event -> {
      }, throwable -> {
      }, () -> channelContextCompleted.set(true));
    });
    // emit close
    ctx0.close();
    // assert that context removed from channel contexs map and cannot emit events
    assertEquals(null, ChannelContext.getIfExist(channelContexts[0].getId()));
    // assert that you can't listen
    assertTrue(channelContextCompleted.get());
  }

  @Test
  public void testDefaultEventStreamSubscribeOnClose() {
    AtomicBoolean eventStreamClosed = new AtomicBoolean();
    eventStream.listenClose(aVoid -> eventStreamClosed.set(true));
    eventStream.close();
    assertTrue(eventStreamClosed.get());
  }

  @Test
  public void testListenChannelContextInactive() {
    eventStream.subscribe(ctx0);
    eventStream.subscribe(ctx1);

    BehaviorSubject<Event> subject = BehaviorSubject.create();
    eventStream.listenChannelContextInactive().subscribe(subject);

    ctx0.close();

    List<Event> list = subject.test().getOnNextEvents();
    assertEquals(1, list.size());
    Event event = list.get(0);
    assertEquals(Topic.ChannelContextInactive, event.getTopic());
    assertEquals(ctx0.getId(), event.getIdentity());

    ctx1.close();

    List<Event> list2 = subject.test().getOnNextEvents();
    assertEquals(1, list2.size());
    Event event2 = list2.get(0);
    assertEquals(Topic.ChannelContextInactive, event2.getTopic());
    assertEquals(ctx1.getId(), event2.getIdentity());
  }
}
