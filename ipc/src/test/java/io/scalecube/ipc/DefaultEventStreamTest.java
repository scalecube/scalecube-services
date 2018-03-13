package io.scalecube.ipc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.scalecube.ipc.Event.Topic;
import io.scalecube.transport.Address;

import org.junit.Test;

import rx.observers.AssertableSubscriber;
import rx.subjects.BehaviorSubject;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class DefaultEventStreamTest {

  private ServiceMessage message0 = ServiceMessage.withQualifier("ok").build();
  private ServiceMessage message1 = ServiceMessage.withQualifier("hola").build();

  private ChannelContext ctx0 = ChannelContext.create(Address.from("localhost:0"));
  private ChannelContext ctx1 = ChannelContext.create(Address.from("localhost:1"));

  private DefaultEventStream eventStream = new DefaultEventStream();

  @Test
  public void testChannelContextPostsEvents() {
    BehaviorSubject<Event> subject = BehaviorSubject.create();
    AssertableSubscriber<Event> subscriber = subject.test();
    eventStream.listen().subscribe(subject);

    eventStream.subscribe(ctx0);
    eventStream.subscribe(ctx1);

    ctx0.postReadSuccess(message0);
    ctx0.postReadSuccess(message0);
    ctx1.postReadSuccess(message1);
    ctx1.postReadSuccess(message1);

    subscriber.assertValueCount(6);
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
  public void testChannelContextUnsubscribingIsIsolated() {
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
    ChannelContext anotherCtx = ChannelContext.create(Address.from("localhost:2"));
    eventStream.subscribe(anotherCtx);
    anotherCtx.postReadError(new RuntimeException("Can't decode incoming msg"));
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
  public void testListenChannelContextClosed() {
    eventStream.subscribe(ctx0);
    eventStream.subscribe(ctx1);

    BehaviorSubject<Event> subject = BehaviorSubject.create();
    eventStream.listenChannelContextClosed().subscribe(subject);

    ctx0.close();

    List<Event> list = subject.test().assertValueCount(1).getOnNextEvents();
    assertEquals(Topic.ChannelContextClosed, list.get(0).getTopic());
    assertEquals(ctx0.getId(), list.get(0).getIdentity());

    ctx1.close();

    List<Event> list2 = subject.test().assertValueCount(1).getOnNextEvents();
    assertEquals(Topic.ChannelContextClosed, list2.get(0).getTopic());
    assertEquals(ctx1.getId(), list2.get(0).getIdentity());
  }

  @Test
  public void testEventStreamSubscribe() {
    BehaviorSubject<Event> subject = BehaviorSubject.create();
    eventStream.listenChannelContextSubscribed().subscribe(subject);

    // subscribe and check events later
    eventStream.subscribe(ctx0);

    List<Event> list = subject.test().assertValueCount(1).getOnNextEvents();
    assertEquals(Topic.ChannelContextSubscribed, list.get(0).getTopic());
    assertEquals(ctx0.getId(), list.get(0).getIdentity());
  }

  @Test
  public void testChannelContextCreateIfAbsentConsumerNotCalled() {
    Address address = Address.from("localhost:8080");
    ChannelContext channelContext = ChannelContext.create(address);
    String id = channelContext.getId();

    AtomicBoolean consumerCalled = new AtomicBoolean();

    // create context on top of exsting context attributes => result from map
    ChannelContext channelContext1 =
        ChannelContext.createIfAbsent(id, address, input -> consumerCalled.set(true));

    assertFalse(consumerCalled.get());
    assertEquals(channelContext, channelContext1);
  }

  @Test
  public void testChannelContextCreateIfAbsentConsumerCalled() {
    String id = "fadsj89fuasd89fa";
    Address address = Address.from("localhost:8080");

    AtomicBoolean consumerCalled = new AtomicBoolean();

    // create context on top of unseen previously context attributes => result new object created
    ChannelContext.createIfAbsent(id, address, input -> consumerCalled.set(true));

    assertTrue(consumerCalled.get());
  }

  @Test
  public void testEventStreamCloseEmitsChannelContextUnsubscribed() {
    eventStream.subscribe(ctx0);
    eventStream.subscribe(ctx1);

    BehaviorSubject<Event> subject = BehaviorSubject.create();
    eventStream.listenChannelContextUnsubscribed().subscribe(subject);
    AssertableSubscriber<Event> subscriber = subject.test();
    eventStream.close();

    List<Event> events = subscriber.assertValueCount(2).getOnNextEvents();
    assertEquals(Topic.ChannelContextUnsubscribed, events.get(0).getTopic());
    assertEquals(Topic.ChannelContextUnsubscribed, events.get(1).getTopic());
  }

  @Test
  public void testChannelContextCloseEmitsChannelContextUnsubscribed() {
    eventStream.subscribe(ctx0);

    BehaviorSubject<Event> subject = BehaviorSubject.create();
    eventStream.listenChannelContextUnsubscribed().subscribe(subject);
    eventStream.listenChannelContextClosed().subscribe(subject);
    AssertableSubscriber<Event> subscriber = subject.test();

    ctx0.close();

    List<Event> events = subscriber.assertValueCount(2).getOnNextEvents();
    assertEquals(Topic.ChannelContextUnsubscribed, events.get(0).getTopic());
    assertEquals(Topic.ChannelContextClosed, events.get(1).getTopic());
  }

  @Test
  public void testEventStreamSubscribeChannelContextSeveralTimes() {
    BehaviorSubject<Event> subject = BehaviorSubject.create();
    eventStream.listen().subscribe(subject);

    AssertableSubscriber<Event> subscriber = subject.test();

    eventStream.subscribe(ctx0);
    eventStream.subscribe(ctx0);
    eventStream.subscribe(ctx0);
    eventStream.subscribe(ctx0);
    eventStream.subscribe(ctx0);

    ctx0.postReadSuccess(message0);
    ctx0.close();

    List<Event> events = subscriber.assertValueCount(4).getOnNextEvents();
    assertEquals(Topic.ChannelContextSubscribed, events.get(0).getTopic());
    assertEquals(Topic.ReadSuccess, events.get(1).getTopic());
    assertEquals(Topic.ChannelContextUnsubscribed, events.get(2).getTopic());
    assertEquals(Topic.ChannelContextClosed, events.get(3).getTopic());
  }
}
