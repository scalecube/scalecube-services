package io.scalecube.ipc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.scalecube.ipc.Event.Topic;
import io.scalecube.transport.Address;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import rx.subjects.BehaviorSubject;
import rx.subjects.ReplaySubject;
import rx.subjects.Subject;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClientStreamTest {

  private ClientStream clientStream;
  private ListeningServerStream serverStream;
  private Address serverAddress;

  @Before
  public void setUp() throws Exception {
    clientStream = ClientStream.newClientStream();
    serverStream = ListeningServerStream.newServerStream().bind();
    serverAddress = serverStream.listenBind().toBlocking().toFuture().get(3, TimeUnit.SECONDS);
  }

  @After
  public void cleanUp() throws Exception {
    clientStream.close();
    serverStream.close();
    serverAddress = serverStream.listenUnbind().toBlocking().toFuture().get(3, TimeUnit.SECONDS);
  }

  @Test
  public void testClientStreamEmitsWriteEvents() throws Exception {
    Subject<Event, Event> emittedEventsSubject = ReplaySubject.create();
    clientStream.listen().subscribe(emittedEventsSubject);

    clientStream.send(serverAddress, ServiceMessage.withQualifier("q/hello").build());

    List<Event> events = emittedEventsSubject.buffer(2).timeout(3, TimeUnit.SECONDS).toBlocking().first();
    assertEquals(2, events.size());
    assertEquals(Topic.Write, events.get(0).getTopic());
    assertEquals(Topic.WriteSuccess, events.get(1).getTopic());
  }

  @Test
  public void testClientStreamSendsToServerStream() throws Exception {
    Subject<Event, Event> requestSubject = ReplaySubject.create();
    serverStream.listen().subscribe(requestSubject);

    clientStream.send(serverAddress, ServiceMessage.withQualifier("hola").build());

    List<Event> events = requestSubject.buffer(1).timeout(3, TimeUnit.SECONDS).toBlocking().first();
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

    clientStream.send(serverAddress, ServiceMessage.withQualifier("echo").build());

    List<Event> events = responseSubject.buffer(1).timeout(3, TimeUnit.SECONDS).toBlocking().first();
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

    Event event = sendFailedSubject.timeout(3, TimeUnit.SECONDS).toBlocking().getIterator().next();
    assertEquals(sendAddress, event.getAddress());
    assertEquals(message, event.getMessageOrThrow());
    assertTrue(event.getError().isPresent());
  }

  @Test
  public void testClientStreamRemotePartyClosed() throws Exception {
    Subject<Event, Event> requestSubject = ReplaySubject.create();
    serverStream.listen().subscribe(requestSubject);

    clientStream.send(serverAddress, ServiceMessage.withQualifier("q/hello").build());

    List<Event> events = requestSubject.buffer(1).timeout(3, TimeUnit.SECONDS).toBlocking().first();
    assertEquals(1, events.size());
    assertEquals(Topic.ReadSuccess, events.get(0).getTopic());

    // close remote party and receive corresp events
    BehaviorSubject<Event> channelInactiveSubject = BehaviorSubject.create();
    clientStream.listenChannelContextClosed().subscribe(channelInactiveSubject);

    // unbind server channel at serverStream
    serverStream.close();
    serverStream.listenUnbind().toBlocking().toFuture().get(3, TimeUnit.SECONDS);

    // await a bit
    TimeUnit.SECONDS.sleep(3);

    // assert that clientStream received event about closed channel corresp to serverStream channel
    Event event = channelInactiveSubject.test().getOnNextEvents().get(0);
    assertEquals(Topic.ChannelContextClosed, event.getTopic());
    assertFalse(event.hasError());
  }
}
