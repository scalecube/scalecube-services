package io.scalecube.ipc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.scalecube.ipc.Event.Topic;
import io.scalecube.transport.Address;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import rx.observers.AssertableSubscriber;
import rx.subjects.BehaviorSubject;
import rx.subjects.ReplaySubject;
import rx.subjects.Subject;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

public class ClientStreamTest {

  private static final Duration TIMEOUT = Duration.ofMillis(3000);
  private static final long TIMEOUT_MILLIS = TIMEOUT.toMillis();

  private ClientStream clientStream;
  private ListeningServerStream serverStream;
  private Address address;

  @Before
  public void setUp() throws Exception {
    clientStream = ClientStream.newClientStream();
    serverStream = ListeningServerStream.newServerStream().bind();
    address = serverStream.listenBind().toBlocking().toFuture().get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
  }

  @After
  public void cleanUp() throws Exception {
    clientStream.close();
    serverStream.close();
    address = serverStream.listenUnbind().toBlocking().toFuture().get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testClientStreamEmitsWriteEvents() throws Exception {
    Subject<Event, Event> subject = BehaviorSubject.create();
    clientStream.listen().subscribe(subject);

    AssertableSubscriber<Event> subscriber = subject.test();

    IntStream.rangeClosed(1, 2)
        .forEach(i -> clientStream.send(address, ServiceMessage.withQualifier("q/" + i).build()));

    List<Event> events = subscriber.awaitValueCount(5, TIMEOUT_MILLIS, TimeUnit.MILLISECONDS).getOnNextEvents();

    assertEquals(Topic.ChannelContextSubscribed, events.get(0).getTopic());
    Event eventWrite1 = events.get(1);
    assertEquals(Topic.Write, eventWrite1.getTopic());
    assertEquals("q/1", eventWrite1.getMessageOrThrow().getQualifier());
    // assertEquals(Topic.Write, eventWrite1.getTopic());
    // assertEquals(Topic.WriteSuccess, eventWrite1.getTopic());
    // assertEquals("q/1", eventWrite1.getMessageOrThrow().getQualifier());
    // Event event2 = events.get(2);
    // assertEquals(Topic.Write, event2.getTopic());
    // assertEquals(Topic.WriteSuccess, event2.getTopic());
    // assertEquals("q/2", event2.getMessageOrThrow().getQualifier());
  }

  @Test
  public void testClientStreamSendsToServerStream() throws Exception {
    Subject<Event, Event> requestSubject = ReplaySubject.create();
    serverStream.listen().subscribe(requestSubject);

    clientStream.send(address, ServiceMessage.withQualifier("hola").build());

    List<Event> events =
        requestSubject.buffer(1).timeout(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS).toBlocking().first();
    assertEquals(1, events.size());
    assertEquals(Topic.ReadSuccess, events.get(0).getTopic());
  }

  @Test
  public void testClientStreamReceivesFromServerStream() throws Exception {
    serverStream.listenReadSuccess()
        .map(Event::getMessageOrThrow)
        .subscribe(serverStream::send);

    Subject<Event, Event> responseSubject = ReplaySubject.create();
    clientStream.listen().filter(Event::isReadSuccess).subscribe(responseSubject);

    clientStream.send(address, ServiceMessage.withQualifier("echo").build());

    List<Event> events =
        responseSubject.buffer(1).timeout(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS).toBlocking().first();
    assertEquals(1, events.size());
    assertEquals(Topic.ReadSuccess, events.get(0).getTopic());
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
    BehaviorSubject<Event> sendFailedSubject = BehaviorSubject.create();
    clientStream.listen().filter(Event::isWriteError).subscribe(sendFailedSubject);

    Address sendAddress = Address.from("host:1234");
    ServiceMessage message = ServiceMessage.withQualifier("q/hello").build();
    clientStream.send(sendAddress, message);

    Event event =
        sendFailedSubject.timeout(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS).toBlocking().getIterator().next();
    assertEquals(sendAddress, event.getAddress());
    assertEquals(message, event.getMessageOrThrow());
    assertTrue(event.getError().isPresent());
  }

  @Test
  public void testClientStreamRemotePartyClosed() throws Exception {
    Subject<Event, Event> requestSubject = ReplaySubject.create();
    serverStream.listen().subscribe(requestSubject);

    clientStream.send(address, ServiceMessage.withQualifier("q/hello").build());

    List<Event> events =
        requestSubject.buffer(1).timeout(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS).toBlocking().first();
    assertEquals(1, events.size());
    assertEquals(Topic.ReadSuccess, events.get(0).getTopic());

    // close remote party and receive corresp events
    BehaviorSubject<Event> channelInactiveSubject = BehaviorSubject.create();
    clientStream.listenChannelContextClosed().subscribe(channelInactiveSubject);

    // unbind server channel at serverStream
    serverStream.close();
    serverStream.listenUnbind().toBlocking().toFuture().get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

    // await a bit
    TimeUnit.MILLISECONDS.sleep(TIMEOUT_MILLIS);

    // assert that clientStream received event about closed channel corresp to serverStream channel
    Event event = channelInactiveSubject.test().getOnNextEvents().get(0);
    assertEquals(Topic.ChannelContextClosed, event.getTopic());
    assertFalse(event.hasError());
  }
}
