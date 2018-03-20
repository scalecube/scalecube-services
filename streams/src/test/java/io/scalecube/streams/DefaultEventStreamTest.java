package io.scalecube.streams;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.scalecube.streams.Event.Topic;
import io.scalecube.transport.Address;

import org.junit.Test;

import rx.observers.AssertableSubscriber;
import rx.subjects.BehaviorSubject;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class DefaultEventStreamTest {

  private StreamMessage messageOne = StreamMessage.builder().qualifier("ok").build();
  private StreamMessage messageTwo = StreamMessage.builder().qualifier("hola").build();

  private ChannelContext ctx = ChannelContext.create(Address.from("localhost:0"));
  private ChannelContext anotherCtx = ChannelContext.create(Address.from("localhost:1"));

  private DefaultEventStream eventStream = new DefaultEventStream();
  private DefaultEventStream anotherEventStream = new DefaultEventStream();

  @Test
  public void testChannelContextPostsEvents() {
    BehaviorSubject<Event> subject = BehaviorSubject.create();
    AssertableSubscriber<Event> subscriber = subject.test();
    eventStream.listen().subscribe(subject);

    eventStream.subscribe(ctx);
    eventStream.subscribe(anotherCtx);

    ctx.postReadSuccess(messageOne);
    ctx.postReadSuccess(messageOne);
    anotherCtx.postReadSuccess(messageTwo);
    anotherCtx.postReadSuccess(messageTwo);

    subscriber.assertValueCount(6);
  }

  @Test
  public void testChannelContextsAreIsolated() {
    BehaviorSubject<Event> subject = BehaviorSubject.create();
    eventStream.listen().subscribe(subject);
    eventStream.subscribe(ctx);
    eventStream.subscribe(anotherCtx);

    ctx.postReadSuccess(messageOne);
    assertEquals(messageOne, subject.test().getOnNextEvents().get(0).getMessageOrThrow());

    // at this point close anotherCtx
    anotherCtx.close();

    // keep posting via ctx
    ctx.postReadSuccess(messageTwo);
    assertEquals(messageTwo, subject.test().getOnNextEvents().get(0).getMessageOrThrow());
  }

  @Test
  public void testChannelContextUnsubscribingIsIsolated() {
    BehaviorSubject<Event> subject = BehaviorSubject.create();
    eventStream.listen().subscribe(subject);
    eventStream.subscribe(ctx);
    eventStream.subscribe(anotherCtx);

    ctx.postReadSuccess(messageOne);
    assertEquals(messageOne, subject.test().getOnNextEvents().get(0).getMessageOrThrow());

    anotherCtx.postReadSuccess(messageTwo);
    assertEquals(messageTwo, subject.test().getOnNextEvents().get(0).getMessageOrThrow());

    ctx.close();
    anotherCtx.close();

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
    ctx.listen().subscribe(event -> {
    }, throwable -> {
    }, () -> eventSubjectClosed.set(true));
    // You can watch-out for close at Observable that was invented for exact reason
    ctx.listenClose(ctx -> channelContextClosed.set(true));
    ctx.close();
    assertTrue(eventSubjectClosed.get());
    assertTrue(channelContextClosed.get());
  }

  @Test
  public void testChannelContextClosedCheckItsState() {
    AtomicBoolean channelContextCompleted = new AtomicBoolean();
    ChannelContext[] channelContexts = new ChannelContext[1];
    ctx.listenClose(ctx -> {
      channelContexts[0] = ctx;
      // try listen
      ctx.listen().subscribe(event -> {
      }, throwable -> {
      }, () -> channelContextCompleted.set(true));
    });
    // emit close
    ctx.close();
    // assert that context removed from channel contexs map and cannot emit events
    assertEquals(null, ChannelContext.getIfExist(channelContexts[0].getId()));
    // assert that you can't listen
    assertTrue(channelContextCompleted.get());
  }

  @Test
  public void testListenChannelContextClosed() {
    eventStream.subscribe(ctx);
    eventStream.subscribe(anotherCtx);

    BehaviorSubject<Event> subject = BehaviorSubject.create();
    eventStream.listenChannelContextClosed().subscribe(subject);

    ctx.close();

    List<Event> list = subject.test().assertValueCount(1).getOnNextEvents();
    assertEquals(Topic.ChannelContextClosed, list.get(0).getTopic());
    assertEquals(ctx.getId(), list.get(0).getIdentity());

    anotherCtx.close();

    List<Event> list2 = subject.test().assertValueCount(1).getOnNextEvents();
    assertEquals(Topic.ChannelContextClosed, list2.get(0).getTopic());
    assertEquals(anotherCtx.getId(), list2.get(0).getIdentity());
  }

  @Test
  public void testEventStreamSubscribe() {
    BehaviorSubject<Event> subject = BehaviorSubject.create();
    eventStream.listenChannelContextSubscribed().subscribe(subject);

    // subscribe and check events later
    eventStream.subscribe(ctx);

    List<Event> list = subject.test().assertValueCount(1).getOnNextEvents();
    assertEquals(Topic.ChannelContextSubscribed, list.get(0).getTopic());
    assertEquals(ctx.getId(), list.get(0).getIdentity());
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
    eventStream.subscribe(ctx);
    eventStream.subscribe(anotherCtx);

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
    eventStream.subscribe(ctx);

    BehaviorSubject<Event> subject = BehaviorSubject.create();
    eventStream.listenChannelContextUnsubscribed().subscribe(subject);
    eventStream.listenChannelContextClosed().subscribe(subject);
    AssertableSubscriber<Event> subscriber = subject.test();

    ctx.close();

    List<Event> events = subscriber.assertValueCount(2).getOnNextEvents();
    assertEquals(Topic.ChannelContextUnsubscribed, events.get(0).getTopic());
    assertEquals(Topic.ChannelContextClosed, events.get(1).getTopic());
  }

  @Test
  public void testEventStreamSubscribeChannelContextSeveralTimes() {
    BehaviorSubject<Event> subject = BehaviorSubject.create();
    eventStream.listen().subscribe(subject);

    AssertableSubscriber<Event> subscriber = subject.test();

    eventStream.subscribe(ctx);
    eventStream.subscribe(ctx);
    eventStream.subscribe(ctx);
    eventStream.subscribe(ctx);
    eventStream.subscribe(ctx);

    ctx.postReadSuccess(messageOne);
    ctx.close();

    List<Event> events = subscriber.assertValueCount(4).getOnNextEvents();
    assertEquals(Topic.ChannelContextSubscribed, events.get(0).getTopic());
    assertEquals(Topic.ReadSuccess, events.get(1).getTopic());
    assertEquals(Topic.ChannelContextUnsubscribed, events.get(2).getTopic());
    assertEquals(Topic.ChannelContextClosed, events.get(3).getTopic());
  }

  @Test
  public void testChannelContextSubscribesToSeveralEventStreams() {
    BehaviorSubject<Event> subject = BehaviorSubject.create();
    BehaviorSubject<Event> anotherSubject = BehaviorSubject.create();

    AssertableSubscriber<Event> subscriber = subject.test();
    AssertableSubscriber<Event> anotherSubscriber = anotherSubject.test();

    eventStream.listen().subscribe(subject);
    anotherEventStream.listen().subscribe(anotherSubject);

    eventStream.subscribe(ctx);
    anotherEventStream.subscribe(ctx);

    List<Event> events = subscriber.assertValueCount(1).getOnNextEvents();
    assertEquals(Topic.ChannelContextSubscribed, events.get(0).getTopic());

    List<Event> anotherEvents = anotherSubscriber.assertValueCount(1).getOnNextEvents();
    assertEquals(Topic.ChannelContextSubscribed, anotherEvents.get(0).getTopic());
  }

  @Test
  public void testChannelContextUnsubscribesFromSeveralEventStreams() {
    BehaviorSubject<Event> subject = BehaviorSubject.create();
    BehaviorSubject<Event> anotherSubject = BehaviorSubject.create();

    eventStream.subscribe(ctx);
    anotherEventStream.subscribe(ctx);

    AssertableSubscriber<Event> subscriber = subject.test();
    AssertableSubscriber<Event> anotherSubscriber = anotherSubject.test();

    eventStream.listen().subscribe(subject);
    anotherEventStream.listen().subscribe(anotherSubject);

    ctx.close();

    List<Event> events = subscriber.assertValueCount(2).getOnNextEvents();
    assertEquals(Topic.ChannelContextUnsubscribed, events.get(0).getTopic());
    assertEquals(Topic.ChannelContextClosed, events.get(1).getTopic());

    List<Event> anotherEvents = anotherSubscriber.assertValueCount(2).getOnNextEvents();
    assertEquals(Topic.ChannelContextUnsubscribed, anotherEvents.get(0).getTopic());
    assertEquals(Topic.ChannelContextClosed, anotherEvents.get(1).getTopic());
  }

  @Test
  public void testChannelContextClosedWithinEventStreamSubscribed() {
    BehaviorSubject<Event> anotherSubject = BehaviorSubject.create();
    anotherEventStream.listen().subscribe(event -> {
      String id = anotherCtx.getId();
      ChannelContext channelContext = ChannelContext.getIfExist(id);
      if (channelContext != null) {
        channelContext.close();
      }
    });
    anotherEventStream.listen().subscribe(anotherSubject);
    AssertableSubscriber<Event> anotherSubscriber = anotherSubject.test();

    anotherEventStream.subscribe(anotherCtx);

    List<Event> anotherEvents = anotherSubscriber.assertValueCount(3).getOnNextEvents();
    assertEquals(Topic.ChannelContextSubscribed, anotherEvents.get(0).getTopic());
    assertEquals(Topic.ChannelContextUnsubscribed, anotherEvents.get(1).getTopic());
    assertEquals(Topic.ChannelContextClosed, anotherEvents.get(2).getTopic());
  }
}
