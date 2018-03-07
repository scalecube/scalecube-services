package io.scalecube.ipc;

import static org.junit.Assert.assertEquals;

import io.scalecube.ipc.Event.Topic;
import io.scalecube.transport.Address;

import org.junit.Test;

import rx.subjects.ReplaySubject;
import rx.subjects.Subject;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class ClientStreamTest {

  @Test
  public void testClientStreamEmitsWriteEvents() throws Exception {
    ListeningServerStream serverStream = ListeningServerStream.newServerStream().bind();
    Address address = serverStream.listenBind().toBlocking().toFuture().get();

    ClientStream clientStream = ClientStream.newClientStream();

    Subject<Event, Event> emittedEventsSubject = ReplaySubject.create();
    clientStream.listen().subscribe(emittedEventsSubject::onNext);

    clientStream.send(address, ServiceMessage.withQualifier("q/hello").build());

    List<Event> events = emittedEventsSubject.buffer(2).timeout(3, TimeUnit.SECONDS).toBlocking().first();
    assertEquals(2, events.size());
    assertEquals(Topic.MessageWrite, events.get(0).getTopic());
    assertEquals(Topic.WriteSuccess, events.get(1).getTopic());
  }

  @Test
  public void testClientStreamSendsToServerStream() throws Exception {
    ListeningServerStream serverStream = ListeningServerStream.newServerStream().bind();
    Address address = serverStream.listenBind().toBlocking().toFuture().get();

    Subject<Event, Event> requestSubject = ReplaySubject.create();
    serverStream.listen().subscribe(requestSubject::onNext);

    ClientStream clientStream = ClientStream.newClientStream();
    clientStream.send(address, ServiceMessage.withQualifier("hola").build());

    List<Event> events = requestSubject.buffer(1).timeout(3, TimeUnit.SECONDS).toBlocking().first();
    assertEquals(1, events.size());
    assertEquals(Topic.ReadSuccess, events.get(0).getTopic());
  }

  @Test
  public void testClientStreamRecvEchoFromServerStream() throws Exception {
    ListeningServerStream serverStream = ListeningServerStream.newServerStream().bind();
    serverStream.listenReadSuccess().subscribe(event -> serverStream.send(event.getMessage().get()));

    ClientStream clientStream = ClientStream.newClientStream();
    Subject<Event, Event> responseSubject = ReplaySubject.create();
    clientStream.listenReadSuccess().subscribe(responseSubject);

    Address address = serverStream.listenBind().toBlocking().toFuture().get();
    clientStream.send(address, ServiceMessage.withQualifier("echo").build());

    List<Event> events = responseSubject.buffer(1).timeout(3, TimeUnit.SECONDS).toBlocking().first();
    assertEquals(1, events.size());
    assertEquals(Topic.ReadSuccess, events.get(0).getTopic());
  }
}
