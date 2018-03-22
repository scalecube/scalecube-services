package io.scalecube.streams;

import static io.scalecube.streams.DefaultStreamProcessor.onCompletedMessage;
import static io.scalecube.streams.DefaultStreamProcessor.onErrorMessage;
import static org.junit.Assert.assertEquals;

import io.scalecube.streams.Event.Topic;
import io.scalecube.transport.Address;

import org.junit.Before;
import org.junit.Test;

import rx.observers.AssertableSubscriber;
import rx.subjects.BehaviorSubject;
import rx.subjects.Subject;

import java.util.List;

public class DefaultStreamProcessorTest {

  private DefaultEventStream eventStream;
  private StreamMessage messageOne;
  private StreamMessage messageTwo;
  private DefaultStreamProcessor streamProcessor;

  @Before
  public void setUp() {
    ChannelContext channelContext = ChannelContext.create(Address.from("localhost:0"));
    eventStream = new DefaultEventStream();
    messageOne = StreamMessage.builder().qualifier("q/1").build();
    messageTwo = StreamMessage.builder().qualifier("q/2").build();
    streamProcessor = new DefaultStreamProcessor(channelContext, eventStream);
  }

  private AssertableSubscriber<Event> listenEventStream() {
    Subject<Event, Event> subject = BehaviorSubject.create();
    eventStream.listen().subscribe(subject);
    return subject.test();
  }

  private void assertObserverEvent(StreamMessage message, Event event) {
    assertEquals(Topic.Write, event.getTopic());
    assertEquals(message, event.getMessageOrThrow());
  }

  @Test
  public void testObserverOnNext() {
    AssertableSubscriber<Event> subscriber1 = listenEventStream();
    streamProcessor.onNext(messageOne);

    List<Event> events = subscriber1.assertValueCount(1)
        .assertNoErrors()
        .assertNotCompleted()
        .getOnNextEvents();
    assertObserverEvent(messageOne, events.get(0));
  }

  @Test
  public void testObserverOnNextSeveralTimes() {
    AssertableSubscriber<Event> subscriber1 = listenEventStream();
    streamProcessor.onNext(messageOne);
    streamProcessor.onNext(messageTwo);

    List<Event> events = subscriber1.assertValueCount(2)
        .assertNoErrors()
        .assertNotCompleted()
        .getOnNextEvents();
    assertObserverEvent(messageOne, events.get(0));
    assertObserverEvent(messageTwo, events.get(1));
  }

  @Test
  public void testObserverOnCompleted() {
    AssertableSubscriber<Event> subscriber1 = listenEventStream();
    streamProcessor.onCompleted();

    List<Event> events = subscriber1.assertValueCount(1)
        .assertNoErrors()
        .assertNotCompleted()
        .getOnNextEvents();
    assertObserverEvent(onCompletedMessage, events.get(0));
  }

  @Test
  public void testObserverOnCompletedSeveralTimes() {
    AssertableSubscriber<Event> subscriber1 = listenEventStream();
    streamProcessor.onCompleted();
    streamProcessor.onCompleted();
    streamProcessor.onCompleted();

    List<Event> events = subscriber1.assertValueCount(1)
        .assertNoErrors()
        .assertNotCompleted()
        .getOnNextEvents();
    assertObserverEvent(onCompletedMessage, events.get(0));
  }

  @Test
  public void testObserverAfterOnCompletedNoEventsEmitted() {
    streamProcessor.onCompleted();

    // ensure after onCompleted events are not emitted
    AssertableSubscriber<Event> subscriber1 = listenEventStream();
    streamProcessor.onNext(messageOne);
    streamProcessor.onNext(messageTwo);
    subscriber1.assertValueCount(0).assertNoErrors().assertNotCompleted();
  }

  @Test
  public void testObserverOnError() {
    AssertableSubscriber<Event> subscriber1 = listenEventStream();
    streamProcessor.onError(new RuntimeException("this is error"));

    List<Event> events = subscriber1.assertValueCount(1)
        .assertNoErrors()
        .assertNotCompleted()
        .getOnNextEvents();
    assertObserverEvent(onErrorMessage, events.get(0));
  }

  @Test
  public void testObserverOnErrorSeveralTimes() {
    AssertableSubscriber<Event> subscriber1 = listenEventStream();
    streamProcessor.onError(new RuntimeException("this is error"));
    streamProcessor.onError(new RuntimeException("this is error"));
    streamProcessor.onError(new RuntimeException("this is error"));

    List<Event> events = subscriber1.assertValueCount(1)
        .assertNoErrors()
        .assertNotCompleted()
        .getOnNextEvents();
    assertObserverEvent(onErrorMessage, events.get(0));
  }

  @Test
  public void testObserverAfterOnErrorNoEventsEmitted() {
    streamProcessor.onError(new RuntimeException("this is error"));

    // ensure after onCompleted events are not emitted
    AssertableSubscriber<Event> subscriber1 = listenEventStream();
    streamProcessor.onNext(messageOne);
    streamProcessor.onNext(messageTwo);
    subscriber1.assertValueCount(0).assertNoErrors().assertNotCompleted();
  }


}
