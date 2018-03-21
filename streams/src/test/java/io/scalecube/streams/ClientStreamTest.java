package io.scalecube.streams;

import static org.hamcrest.core.AnyOf.anyOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

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
  public void setUp() throws Exception {
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
    Subject<Event, Event> clientStreamSubject = BehaviorSubject.create();
    clientStream.listen().subscribe(clientStreamSubject);
    AssertableSubscriber<Event> clientStreamSubscriber = clientStreamSubject.test();

    IntStream.rangeClosed(1, 5)
        .forEach(i -> clientStream.send(address, StreamMessage.builder().qualifier("q/" + i).build()));

    List<Event> events =
        clientStreamSubscriber.awaitValueCount(11, TIMEOUT_MILLIS, TimeUnit.MILLISECONDS).getOnNextEvents();

    assertEquals(Topic.ChannelContextSubscribed, events.get(0).getTopic());
    assertWrite("q/1", events.get(1));
    assertWriteSuccess("q/1", events.get(2));
    assertWrite("q/2", events.get(3));
    assertWriteSuccess("q/2", events.get(4));
    assertWrite("q/3", events.get(5));
    assertWriteSuccess("q/3", events.get(6));
    assertWrite("q/4", events.get(7));
    assertWriteSuccess("q/4", events.get(8));
    assertWrite("q/5", events.get(9));
    assertWriteSuccess("q/5", events.get(10));
  }

  @Test
  public void testClientStreamSendsToServerStream() throws Exception {
    Subject<Event, Event> serverStreamSubject = BehaviorSubject.create();
    serverStream.listen().subscribe(serverStreamSubject);
    AssertableSubscriber<Event> clientStreamSubscriber = serverStreamSubject.test();

    IntStream.rangeClosed(1, 5)
        .forEach(i -> clientStream.send(address, StreamMessage.builder().qualifier("hola/" + i).build()));

    List<Event> events =
        clientStreamSubscriber.awaitValueCount(6, TIMEOUT_MILLIS, TimeUnit.MILLISECONDS).getOnNextEvents();

    assertEquals(Topic.ChannelContextSubscribed, events.get(0).getTopic());
    assertReadSuccess("hola/1", events.get(1));
    assertReadSuccess("hola/2", events.get(2));
    assertReadSuccess("hola/3", events.get(3));
    assertReadSuccess("hola/4", events.get(4));
    assertReadSuccess("hola/5", events.get(5));
  }

  @Test
  public void testClientStreamReceivesFromServerStream() throws Exception {
    serverStream.listenReadSuccess()
        .map(Event::getMessageOrThrow)
        .subscribe(serverStream::send);

    Subject<Event, Event> clientStreamSubject = BehaviorSubject.create();
    clientStream.listen().filter(Event::isReadSuccess).subscribe(clientStreamSubject);
    AssertableSubscriber<Event> clientStreamSubscriber = clientStreamSubject.test();

    IntStream.rangeClosed(1, 5)
        .forEach(i -> clientStream.send(address, StreamMessage.builder().qualifier("hola/" + i).build()));

    List<Event> events =
        clientStreamSubscriber.awaitValueCount(5, TIMEOUT_MILLIS, TimeUnit.MILLISECONDS).getOnNextEvents();

    assertReadSuccess("hola/1", events.get(0));
    assertReadSuccess("hola/2", events.get(1));
    assertReadSuccess("hola/3", events.get(2));
    assertReadSuccess("hola/4", events.get(3));
    assertReadSuccess("hola/5", events.get(4));
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
    BehaviorSubject<Event> clientStreamSubject = BehaviorSubject.create();
    clientStream.listen().filter(Event::isWriteError).subscribe(clientStreamSubject);
    AssertableSubscriber<Event> clientStreamSubscriber = clientStreamSubject.test();

    Address failedAddress = Address.from("host:1234");
    StreamMessage message = StreamMessage.builder().qualifier("q/helloFail").build();
    clientStream.send(failedAddress, message);

    Event event =
        clientStreamSubscriber.awaitValueCount(1, TIMEOUT_MILLIS, TimeUnit.MILLISECONDS).getOnNextEvents().get(0);

    assertEquals(failedAddress, event.getAddress());
    assertEquals(message, event.getMessageOrThrow());
    assertTrue("An error must be here", event.getError().isPresent());
  }

  @Test
  public void testClientStreamRemotePartyClosed() throws Exception {
    Subject<Event, Event> serverStreamSubject = BehaviorSubject.create();
    serverStream.listen().subscribe(serverStreamSubject);
    AssertableSubscriber<Event> serverStreamSubscriber = serverStreamSubject.test();

    clientStream.send(address, StreamMessage.builder().qualifier("q/hello").build());

    List<Event> events =
        serverStreamSubscriber.awaitValueCount(2, TIMEOUT_MILLIS, TimeUnit.MILLISECONDS).getOnNextEvents();
    assertEquals(Topic.ChannelContextSubscribed, events.get(0).getTopic());
    assertEquals(Topic.ReadSuccess, events.get(1).getTopic());

    // close remote party and receive corresp events
    BehaviorSubject<Event> clientStreamChannelContextInactiveSubject = BehaviorSubject.create();
    clientStream.listenChannelContextClosed().subscribe(clientStreamChannelContextInactiveSubject);

    // unbind server channel at serverStream
    serverStream.close();

    // await a bit
    TimeUnit.MILLISECONDS.sleep(TIMEOUT_MILLIS);

    // assert that clientStream received event about closed channel corresp to serverStream channel
    Event event = clientStreamChannelContextInactiveSubject.test().getOnNextEvents().get(0);
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
      Subject<Event, Event> clientStreamSubject = BehaviorSubject.create();
      clientStream.listenChannelContextSubscribed().subscribe(clientStreamSubject);
      AssertableSubscriber<Event> clientStreamSubscriber = clientStreamSubject.test();
      // send msgs
      clientStream.send(address, StreamMessage.builder().qualifier("q/msg").build());
      clientStream.send(anotherAddress, StreamMessage.builder().qualifier("q/anotherMsg").build());

      List<Event> events =
          clientStreamSubscriber.awaitValueCount(2, TIMEOUT_MILLIS, TimeUnit.MILLISECONDS).getOnNextEvents();

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
