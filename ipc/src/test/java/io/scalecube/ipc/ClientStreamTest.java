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

  private ListeningServerStream serverStream;
  private Address address;

  @Before
  public void setUp() throws Exception {
    serverStream = ListeningServerStream.newServerStream().bind();
    address = serverStream.listenBind().toBlocking().toFuture().get(3, TimeUnit.SECONDS);
  }

  @After
  public void cleanUp() throws Exception {
    serverStream.close();
    address = serverStream.listenUnbind().toBlocking().toFuture().get(3, TimeUnit.SECONDS);
  }

  @Test
  public void testClientStreamEmitsWriteEvents() throws Exception {
    ClientStream clientStream = ClientStream.newClientStream();

    Subject<Event, Event> emittedEventsSubject = ReplaySubject.create();
    clientStream.listen().subscribe(emittedEventsSubject);

    clientStream.send(address, ServiceMessage.withQualifier("q/hello").build());

    List<Event> events = emittedEventsSubject.buffer(2).timeout(3, TimeUnit.SECONDS).toBlocking().first();
    assertEquals(2, events.size());
    assertEquals(Topic.MessageWrite, events.get(0).getTopic());
    assertEquals(Topic.WriteSuccess, events.get(1).getTopic());
  }

  @Test
  public void testClientStreamSendsToServerStream() throws Exception {
    Subject<Event, Event> requestSubject = ReplaySubject.create();
    serverStream.listen().subscribe(requestSubject);

    ClientStream clientStream = ClientStream.newClientStream();
    clientStream.send(address, ServiceMessage.withQualifier("hola").build());

    List<Event> events = requestSubject.buffer(1).timeout(3, TimeUnit.SECONDS).toBlocking().first();
    assertEquals(1, events.size());
    assertEquals(Topic.ReadSuccess, events.get(0).getTopic());
  }

  @Test
  public void testClientStreamReceivesFromServerStream() throws Exception {
    serverStream.listenMessageReadSuccess().subscribe(serverStream::send);

    ClientStream clientStream = ClientStream.newClientStream();
    Subject<Event, Event> responseSubject = ReplaySubject.create();
    clientStream.listen().filter(Event::isReadSuccess).subscribe(responseSubject);

    clientStream.send(address, ServiceMessage.withQualifier("echo").build());

    List<Event> events = responseSubject.buffer(1).timeout(3, TimeUnit.SECONDS).toBlocking().first();
    assertEquals(1, events.size());
    assertEquals(Topic.ReadSuccess, events.get(0).getTopic());
  }

  @Test
  public void testClientStreamOnClose() throws Exception {
    ClientStream clientStream = ClientStream.newClientStream();
    AtomicBoolean onCloseBoolean = new AtomicBoolean();
    clientStream.listenClose(aVoid -> onCloseBoolean.set(true));

    clientStream.send(address, ServiceMessage.withQualifier("q/hello").build());

    Subject<Event, Event> emittedEventsSubject = ReplaySubject.create();
    clientStream.listen().subscribe(emittedEventsSubject);

    List<Event> events = emittedEventsSubject.buffer(2).timeout(3, TimeUnit.SECONDS).toBlocking().first();
    assertEquals(2, events.size());
    assertEquals(Topic.MessageWrite, events.get(0).getTopic());
    assertEquals(Topic.WriteSuccess, events.get(1).getTopic());

    clientStream.close();

    assertTrue(onCloseBoolean.get());
  }

  @Test
  public void testClientStreamOnCloseRemotePartyClosed() throws Exception {
    ClientStream clientStream = ClientStream.newClientStream();

    clientStream.send(address, ServiceMessage.withQualifier("q/hello").build());

    Subject<Event, Event> emittedEventsSubject = ReplaySubject.create();
    clientStream.listen().subscribe(emittedEventsSubject);

    List<Event> events = emittedEventsSubject.buffer(2).timeout(3, TimeUnit.SECONDS).toBlocking().first();
    assertEquals(2, events.size());
    assertEquals(Topic.MessageWrite, events.get(0).getTopic());
    assertEquals(Topic.WriteSuccess, events.get(1).getTopic());

    // close remote party and receive corresp events
    BehaviorSubject<Event> channelInactiveSubject = BehaviorSubject.create();
    clientStream.listenChannelContextInactive().subscribe(channelInactiveSubject);
    // unbind server channel at serverStream
    serverStream.close();
    serverStream.listenUnbind().toBlocking().toFuture().get(3, TimeUnit.SECONDS);
    // await a bit
    TimeUnit.SECONDS.sleep(3);
    // assert that clientStream received events about closed channels corresp to serverStream channels
    Event event = channelInactiveSubject.test().getOnNextEvents().get(0);
    assertEquals(Topic.ChannelContextInactive, event.getTopic());
    assertFalse(event.hasError());
  }
}
