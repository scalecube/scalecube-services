package io.scalecube.streams;

import static org.hamcrest.core.AnyOf.anyOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

import io.scalecube.streams.Event.Topic;
import io.scalecube.transport.Address;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import rx.observers.AssertableSubscriber;
import rx.subjects.BehaviorSubject;
import rx.subjects.Subject;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

public class ClientStreamTest {

  private static final Duration TIMEOUT = Duration.ofMillis(3000);
  private static final long TIMEOUT_MILLIS = TIMEOUT.toMillis();

  private Address address;
  private ClientStream clientStream;
  private ListeningServerStream serverStream =
      ListeningServerStream.newListeningServerStream().withListenAddress("localhost");

  @Before
  public void setUp() {
    clientStream = ClientStream.newClientStream();
    address = serverStream.bindAwait();
  }

  @After
  public void cleanUp() throws Exception {
    clientStream.close();
    serverStream.close();
  }

  private void assertWrite(String q, Event event) {
    assertEquals(Topic.Write, event.getTopic());
    assertEquals(q, event.getMessageOrThrow().qualifier());
  }

  private void assertWriteSuccess(String q, Event event) {
    assertEquals(Topic.WriteSuccess, event.getTopic());
    assertEquals(q, event.getMessageOrThrow().qualifier());
  }

  private void assertReadSuccess(String q, Event event) {
    assertEquals(Topic.ReadSuccess, event.getTopic());
    assertEquals(q, event.getMessageOrThrow().qualifier());
  }

  @Test
  public void testClientStreamProducesWriteEvents() throws Exception {
    Subject<Event, Event> clientSubject = BehaviorSubject.create();
    clientStream.listen().subscribe(clientSubject);
    AssertableSubscriber<Event> clientSubscriber = clientSubject.test();

    int n = (int) 1e4;
    IntStream.rangeClosed(1, n)
        .forEach(i -> clientStream.send(address, StreamMessage.builder().qualifier("q/" + i).build()));

    int expected = n * 2 + 1;
    List<Event> events = clientSubscriber
        .awaitValueCount(expected, TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        .assertNoTerminalEvent()
        .getOnNextEvents();

    assertEquals(Topic.ChannelContextSubscribed, events.get(0).getTopic());
    for (int w = 1, ws = 2, q = 1; q <= expected && w <= expected - 2 && ws <= expected - 2; w += 2, ws += 2, q++) {
      assertWrite("q/" + q, events.get(w));
      assertWriteSuccess("q/" + q, events.get(ws));
    }
  }

  @Test
  public void testClientStreamSendsToServerStream() throws Exception {
    Subject<Event, Event> serverSubject = BehaviorSubject.create();
    serverStream.listen().subscribe(serverSubject);
    AssertableSubscriber<Event> serverSubscriber = serverSubject.test();

    int n = (int) 1e4;
    IntStream.rangeClosed(1, n)
        .forEach(i -> clientStream.send(address, StreamMessage.builder().qualifier("q/" + i).build()));

    int expected = n + 1;
    List<Event> events = serverSubscriber
        .awaitValueCount(expected, TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        .assertNoTerminalEvent()
        .getOnNextEvents();

    assertEquals(Topic.ChannelContextSubscribed, events.get(0).getTopic());
    for (int i = 1; i < expected; i++) {
      assertReadSuccess("q/" + i, events.get(i));
    }
  }

  @Test
  public void testClientStreamReceivesFromServerStream() throws Exception {
    serverStream.listenReadSuccess()
        .map(Event::getMessageOrThrow)
        .subscribe(serverStream::send);

    Subject<Event, Event> clientSubject = BehaviorSubject.create();
    clientStream.listen().filter(Event::isReadSuccess).subscribe(clientSubject);
    AssertableSubscriber<Event> clientSubscriber = clientSubject.test();

    int n = (int) 1e4;
    IntStream.rangeClosed(1, n)
        .forEach(i -> clientStream.send(address, StreamMessage.builder().qualifier("q/" + i).build()));

    List<Event> events = clientSubscriber
        .awaitValueCount(n, TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        .assertNoTerminalEvent()
        .getOnNextEvents();

    for (int i = 1; i <= n; i++) {
      assertReadSuccess("q/" + i, events.get(i - 1));
    }
  }

  @Test
  public void testClientStreamOnClose() throws Exception {
    AtomicBoolean onCloseBoolean = new AtomicBoolean();
    clientStream.listenClose(aVoid -> onCloseBoolean.set(true));
    clientStream.close();
    assertTrue(onCloseBoolean.get());
  }

  @Test
  public void testClientStreamSendFailed() throws Exception {
    BehaviorSubject<Event> clientSubject = BehaviorSubject.create();
    clientStream.listen().filter(Event::isWriteError).subscribe(clientSubject);
    AssertableSubscriber<Event> clientSubscriber = clientSubject.test();

    Address failedAddress = Address.from("host:1234");
    StreamMessage message = StreamMessage.builder().qualifier("q/helloFail").build();
    clientStream.send(failedAddress, message);

    Event event = clientSubscriber
        .awaitValueCount(1, TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        .assertNoTerminalEvent()
        .getOnNextEvents()
        .get(0);

    assertEquals(failedAddress, event.getAddress());
    assertEquals(message, event.getMessageOrThrow());
    assertTrue("An error must be here", event.getError().isPresent());
  }

  @Test
  public void testClientStreamRemotePartyClosed() throws Exception {
    Subject<Event, Event> serverSubject = BehaviorSubject.create();
    serverStream.listen().subscribe(serverSubject);
    AssertableSubscriber<Event> serverSubscriber = serverSubject.test();

    clientStream.send(address, StreamMessage.builder().qualifier("q/hello").build());

    List<Event> events = serverSubscriber
        .awaitValueCount(2, TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        .assertNoTerminalEvent()
        .getOnNextEvents();

    assertEquals(Topic.ChannelContextSubscribed, events.get(0).getTopic());
    assertEquals(Topic.ReadSuccess, events.get(1).getTopic());

    // close remote party and receive corresp events
    BehaviorSubject<Event> clientChannelContextInactiveSubject = BehaviorSubject.create();
    clientStream.listenChannelContextClosed().subscribe(clientChannelContextInactiveSubject);

    // unbind server channel at serverStream
    serverStream.close();

    // await a bit
    TimeUnit.MILLISECONDS.sleep(TIMEOUT_MILLIS);

    // assert that clientStream received event about closed channel corresp to serverStream channel
    Event event = clientChannelContextInactiveSubject.test().getOnNextEvents().get(0);
    assertEquals(Topic.ChannelContextClosed, event.getTopic());
    assertFalse("Must not have error at this point", event.hasError());
  }

  @Test
  public void testClientStreamManagesConnections() throws Exception {
    ListeningServerStream anotherServerStream =
        ListeningServerStream.newListeningServerStream().withListenAddress("localhost");
    Address anotherAddress = anotherServerStream.bindAwait();

    try {
      // send two msgs on two addresses => activate two connections => emit events
      Subject<Event, Event> clientSubject = BehaviorSubject.create();
      clientStream.listenChannelContextSubscribed().subscribe(clientSubject);
      AssertableSubscriber<Event> clientSubscriber = clientSubject.test();
      // send msgs
      clientStream.send(address, StreamMessage.builder().qualifier("q/msg").build());
      clientStream.send(anotherAddress, StreamMessage.builder().qualifier("q/anotherMsg").build());

      List<Event> events = clientSubscriber.awaitValueCount(2, TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
          .assertNoTerminalEvent()
          .getOnNextEvents();

      Event firstEvent = events.get(0);
      Event secondEvent = events.get(1);
      assertEquals(Topic.ChannelContextSubscribed, firstEvent.getTopic());
      assertEquals(Topic.ChannelContextSubscribed, secondEvent.getTopic());
      assertThat(firstEvent.getAddress(), anyOf(is(address), is(anotherAddress)));
      assertThat(secondEvent.getAddress(), anyOf(is(address), is(anotherAddress)));

      // close and ensure all connections closed
      Subject<Event, Event> closeSubject = BehaviorSubject.create();
      clientStream.listenChannelContextUnsubscribed().subscribe(closeSubject);
      // close
      AssertableSubscriber<Event> closeSubscriber = closeSubject.test();
      clientStream.close();

      List<Event> closeEvents = new ArrayList<>(closeSubscriber
          .awaitValueCount(2, TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
          .getOnNextEvents());

      Event firstCloseEvent = closeEvents.get(0);
      Event secondCloseEvent = closeEvents.get(1);

      assertEquals(Topic.ChannelContextUnsubscribed, firstCloseEvent.getTopic());
      assertEquals(Topic.ChannelContextUnsubscribed, secondCloseEvent.getTopic());
      assertThat(firstCloseEvent.getAddress(), anyOf(is(address), is(anotherAddress)));
      assertThat(secondCloseEvent.getAddress(), anyOf(is(address), is(anotherAddress)));
    } finally {
      anotherServerStream.close();
    }
  }
}
