package io.scalecube.streams;

import static org.junit.Assert.assertEquals;

import io.scalecube.streams.Event.Topic;
import io.scalecube.streams.exceptions.DefaultStreamExceptionMapper;
import io.scalecube.streams.exceptions.InternalStreamException;
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
  private ChannelContext channelContext;

  @Before
  public void setUp() {
    channelContext = ChannelContext.create(Address.from("localhost:0"));
    eventStream = new DefaultEventStream();
    messageOne = StreamMessage.builder().qualifier("q/1").build();
    messageTwo = StreamMessage.builder().qualifier("q/2").build();
    streamProcessor = new DefaultStreamProcessor(channelContext, eventStream, new DefaultStreamExceptionMapper());
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
    ErrorData errorData = new ErrorData(132, "something happened");
    StreamMessage expectedMsg = StreamMessage.builder()
        .qualifier(Qualifier.error(Qualifier.Q_GENERAL_FAILURE.getAction()))
        .data(errorData)
        .build();

    AssertableSubscriber<Event> subscriber1 = listenEventStream();
    streamProcessor.onError(new InternalStreamException(errorData));
    subscriber1.assertValueCount(1).assertNoErrors().assertNotCompleted();
    assertWriteEvent(expectedMsg, subscriber1.getOnNextEvents());

    // ensure after onError events are not emitted
    AssertableSubscriber<Event> subscriber2 = listenEventStream();
    streamProcessor.onNext(messageOne);
    subscriber2.assertValueCount(0).assertNoErrors().assertNotCompleted();
  }

  @Test
  public void testObserverOnNextWithErrorData() {
    ErrorData errorData = new ErrorData(132, "something happened");
    StreamMessage expectedMessage = StreamMessage.builder()
        .qualifier(Qualifier.error(Qualifier.Q_GENERAL_FAILURE.getAction()))
        .data(errorData)
        .build();

    AssertableSubscriber<Event> subscriber1 = listenEventStream();
    streamProcessor.onNext(expectedMessage);
    subscriber1.assertValueCount(1).assertNoErrors().assertNotCompleted();
    assertWriteEvent(expectedMessage, subscriber1.getOnNextEvents());
  }

  @Test
  public void testChannelContextOnReadSuccessWithErrorQulifier() {
    ErrorData errorData = new ErrorData(132, "something happened");
    StreamMessage expectedMessage = StreamMessage.builder()
        .qualifier(Qualifier.error(Qualifier.Q_GENERAL_FAILURE.getAction()))
        .data(errorData)
        .build();

    AssertableSubscriber<Event> subscriber1 = listenEventStream();
    AssertableSubscriber<StreamMessage> streamProcessorSubscriber = listenStreamProcessor();
    channelContext.postReadSuccess(expectedMessage);

    subscriber1.assertValueCount(1).assertNoErrors().assertNotCompleted();
    assertReadSuccessEvent(expectedMessage, subscriber1.getOnNextEvents());

    streamProcessorSubscriber.assertNoValues().assertError(InternalStreamException.class);
  }

  @Test
  public void testChannelContextOnReadSuccessWithOnCompletedQulifier() {
    StreamMessage expectedMessage = StreamMessage.builder().qualifier(Qualifier.Q_ON_COMPLETED).build();

    AssertableSubscriber<Event> subscriber1 = listenEventStream();
    AssertableSubscriber<StreamMessage> streamProcessorSubscriber = listenStreamProcessor();
    channelContext.postReadSuccess(expectedMessage);

    subscriber1.assertValueCount(1).assertNoErrors().assertNotCompleted();
    assertReadSuccessEvent(expectedMessage, subscriber1.getOnNextEvents());

    streamProcessorSubscriber.assertNoValues().assertNoErrors().assertCompleted();
  }

  @Test
  public void testChannelContextOnReadSuccessWithoutCompletedOrErrorQulifier() {
    StreamMessage expectedMessage = StreamMessage.builder().qualifier("q/onNext").build();

    AssertableSubscriber<Event> subscriber1 = listenEventStream();
    AssertableSubscriber<StreamMessage> streamProcessorSubscriber = listenStreamProcessor();
    channelContext.postReadSuccess(expectedMessage);

    subscriber1.assertValueCount(1).assertNoErrors().assertNotCompleted();
    assertReadSuccessEvent(expectedMessage, subscriber1.getOnNextEvents());

    streamProcessorSubscriber.assertValueCount(1).assertNoErrors().assertNotCompleted();
    assertEquals(expectedMessage, streamProcessorSubscriber.getOnNextEvents().get(0));
  }

  private AssertableSubscriber<Event> listenEventStream() {
    Subject<Event, Event> subject = BehaviorSubject.create();
    eventStream.listen().subscribe(subject);
    return subject.test();
  }

  private AssertableSubscriber<StreamMessage> listenStreamProcessor() {
    Subject<StreamMessage, StreamMessage> subject = BehaviorSubject.create();
    streamProcessor.listen().subscribe(subject);
    return subject.test();
  }

  private void assertWriteEvent(StreamMessage message, List<Event> events) {
    assertEquals(Topic.Write, events.get(0).getTopic());
    assertEquals(message, events.get(0).getMessageOrThrow());
  }

  private void assertReadSuccessEvent(StreamMessage message, List<Event> events) {
    assertEquals(Topic.ReadSuccess, events.get(0).getTopic());
    assertEquals(message, events.get(0).getMessageOrThrow());
  }
}
