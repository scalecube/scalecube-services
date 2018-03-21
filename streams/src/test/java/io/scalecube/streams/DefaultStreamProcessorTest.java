package io.scalecube.streams;

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

  private void assertWriteEvent(StreamMessage message, List<Event> events) {
    assertEquals(Topic.Write, events.get(0).getTopic());
    assertEquals(message, events.get(0).getMessageOrThrow());
  }

  @Test
  public void testObserverOnNext() {
    AssertableSubscriber<Event> subscriber1 = listenEventStream();
    streamProcessor.onNext(messageOne);
    subscriber1.assertValueCount(1).assertNoErrors().assertNotCompleted();
    assertWriteEvent(messageOne, subscriber1.getOnNextEvents());

    // call onNext one more time and check for emitted event
    AssertableSubscriber<Event> subscriber2 = listenEventStream();
    streamProcessor.onNext(messageTwo);
    subscriber2.assertValueCount(1).assertNoErrors().assertNotCompleted();
    assertWriteEvent(messageTwo, subscriber2.getOnNextEvents());
  }

  @Test
  public void testObserverOnCompleted() {
    AssertableSubscriber<Event> subscriber1 = listenEventStream();
    streamProcessor.onCompleted();
    subscriber1.assertValueCount(1).assertNoErrors().assertNotCompleted();
    assertWriteEvent(DefaultStreamProcessor.onCompletedMessage, subscriber1.getOnNextEvents());

    // ensure after onCompleted events are not emitted
    AssertableSubscriber<Event> subscriber2 = listenEventStream();
    streamProcessor.onNext(messageOne);
    subscriber2.assertValueCount(0).assertNoErrors().assertNotCompleted();
  }

  @Test
  public void testObserverOnError() {
    AssertableSubscriber<Event> subscriber1 = listenEventStream();
    streamProcessor.onError(new RuntimeException("exception!"));
    subscriber1.assertValueCount(1).assertNoErrors().assertNotCompleted();
    assertWriteEvent(DefaultStreamProcessor.onErrorMessage, subscriber1.getOnNextEvents());

    // ensure after onError events are not emitted
    AssertableSubscriber<Event> subscriber2 = listenEventStream();
    streamProcessor.onNext(messageOne);
    subscriber2.assertValueCount(0).assertNoErrors().assertNotCompleted();
  }
}
