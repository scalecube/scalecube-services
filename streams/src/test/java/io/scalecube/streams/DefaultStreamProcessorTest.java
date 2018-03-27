package io.scalecube.streams;

import static io.scalecube.streams.DefaultStreamProcessor.onCompletedMessage;
import static io.scalecube.streams.DefaultStreamProcessor.onErrorMessage;
import static org.junit.Assert.assertEquals;

import io.scalecube.streams.Event.Topic;
import io.scalecube.transport.Address;

import org.junit.Before;
import org.junit.Test;

import rx.Observable;
import rx.observers.AssertableSubscriber;

import java.io.IOException;
import java.net.ConnectException;
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
    streamProcessor = new DefaultStreamProcessor(channelContext, eventStream);
  }

  private void assertObserverEvent(StreamMessage message, Event event) {
    assertEquals(Topic.Write, event.getTopic());
    assertEquals(message, event.getMessageOrThrow());
  }

  @Test
  public void testObserverOnNext() {
    AssertableSubscriber<Event> subscriber1 = eventStream.listen().test();
    streamProcessor.onNext(messageOne);

    List<Event> events = subscriber1.assertValueCount(1)
        .assertNoErrors()
        .assertNotCompleted()
        .getOnNextEvents();
    assertObserverEvent(messageOne, events.get(0));
  }

  @Test
  public void testObserverOnNextSeveralTimes() {
    AssertableSubscriber<Event> subscriber1 = eventStream.listen().test();
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
    AssertableSubscriber<Event> subscriber1 = eventStream.listen().test();
    streamProcessor.onCompleted();

    List<Event> events = subscriber1.assertValueCount(1)
        .assertNoErrors()
        .assertNotCompleted()
        .getOnNextEvents();
    assertObserverEvent(onCompletedMessage, events.get(0));
  }

  @Test
  public void testObserverOnCompletedSeveralTimes() {
    AssertableSubscriber<Event> subscriber1 = eventStream.listen().test();
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
    AssertableSubscriber<Event> subscriber1 = eventStream.listen().test();
    streamProcessor.onNext(messageOne);
    streamProcessor.onNext(messageTwo);
    subscriber1.assertValueCount(0).assertNoErrors().assertNotCompleted();
  }

  @Test
  public void testObserverOnError() {
    AssertableSubscriber<Event> subscriber1 = eventStream.listen().test();
    streamProcessor.onError(new RuntimeException("this is error"));

    List<Event> events = subscriber1.assertValueCount(1)
        .assertNoErrors()
        .assertNotCompleted()
        .getOnNextEvents();
    assertObserverEvent(onErrorMessage, events.get(0));
  }

  @Test
  public void testObserverOnErrorSeveralTimes() {
    AssertableSubscriber<Event> subscriber1 = eventStream.listen().test();
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
    AssertableSubscriber<Event> subscriber1 = eventStream.listen().test();
    streamProcessor.onNext(messageOne);
    streamProcessor.onNext(messageTwo);
    subscriber1.assertValueCount(0).assertNoErrors().assertNotCompleted();
  }

  @Test
  public void testObservableOnNext() {
    AssertableSubscriber<StreamMessage> subscriber = streamProcessor.listen().test();
    channelContext.postReadSuccess(messageOne);
    channelContext.postReadSuccess(messageTwo);
    subscriber.assertValues(messageOne, messageTwo).assertNoTerminalEvent();
  }

  @Test
  public void testObservableTerminalEvent() {
    // check onCompleted
    AssertableSubscriber<StreamMessage> completedSubscriber = streamProcessor.listen().test();
    channelContext.postReadSuccess(onCompletedMessage);
    completedSubscriber.assertTerminalEvent()
        .assertCompleted()
        .assertNoErrors()
        .assertNoValues()
        .assertUnsubscribed();

    // check onError
    AssertableSubscriber<StreamMessage> errorSubscriber = streamProcessor.listen().test();
    channelContext.postReadSuccess(onErrorMessage);
    errorSubscriber.assertTerminalEvent()
        .assertError(IOException.class)
        .assertNoValues()
        .assertNotCompleted()
        .assertUnsubscribed();
  }

  @Test
  public void testObservableWriteError() {
    // emulate connect exception
    AssertableSubscriber<StreamMessage> subscriber = streamProcessor.listen().test();
    ConnectException exception = new ConnectException("Connect failed");
    channelContext.postWriteError(messageOne, exception);

    // check onError
    subscriber.assertTerminalEvent()
        .assertError(exception)
        .assertNoValues()
        .assertNotCompleted()
        .assertUnsubscribed();
  }

  @Test
  public void testObservableWriteErrorThenRetry() {
    // setup retry logic
    IOException exception = new IOException("Connection closed");
    Observable<StreamMessage> observable1 =
        streamProcessor.listen().retry((i, throwable) -> throwable == exception);

    // emulate message write error
    AssertableSubscriber<StreamMessage> writeErrorSubscriber = observable1.test();
    channelContext.postWriteError(messageOne, exception);

    // check retry worked and we still have alive subscription
    writeErrorSubscriber.assertNoTerminalEvent().assertNoValues();
  }

  @Test
  public void testObservableRemoteChannelContextClosed() {
    // create remote channel context and subscribe it on event stream
    ChannelContext remoteChannelContext = ChannelContext.create(Address.from("localhost:6060"));
    eventStream.subscribe(remoteChannelContext);

    AssertableSubscriber<StreamMessage> subscriber = streamProcessor.listen().test();

    // emulate connection closed by remote party
    remoteChannelContext.close();

    // check onError
    subscriber.assertTerminalEvent()
        .assertError(IOException.class)
        .assertNoValues()
        .assertNotCompleted()
        .assertUnsubscribed();
  }
}
