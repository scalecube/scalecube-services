package io.scalecube.ipc;

import static io.scalecube.ipc.Event.Topic;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.scalecube.transport.Address;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import rx.subjects.ReplaySubject;
import rx.subjects.Subject;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ListeningServerStreamTest {

  private Address address;
  private ListeningServerStream templateServerStream;

  @Before
  public void setUp() {
    address = Address.create("127.0.0.1", 5801);
    templateServerStream = ListeningServerStream.newServerStream().withListenAddress("localhost");
  }

  @After
  public void cleanUp() {
    templateServerStream.close();
  }

  @Test
  public void testServerStreamListenBindNotReturnIfBindNotCalled() throws Exception {
    // issue server stream bind
    ListeningServerStream serverStream = templateServerStream.bind();
    // check bind on wrong reference
    try {
      templateServerStream.listenBind().toBlocking().toFuture().get(1, TimeUnit.SECONDS);
      fail("Expected TimeoutException");
    } catch (Exception e) {
      assertEquals(TimeoutException.class, e.getClass());
    }
    // check stream is bound on correct reference
    assertEquals(address, serverStream.listenBind().toBlocking().toFuture().get());
  }

  @Test
  public void testServerStreamListenUnbindNotReturnIfCloseNotCalled() throws Exception {
    // check stream is bound
    ListeningServerStream serverStream = templateServerStream.bind();
    assertEquals(address, serverStream.listenBind().toBlocking().toFuture().get());
    // issue close on reference which is not bound
    templateServerStream.close();
    try {
      templateServerStream.listenUnbind().toBlocking().toFuture().get(1, TimeUnit.SECONDS);
      fail("Expected TimeoutException");
    } catch (Exception e) {
      assertEquals(TimeoutException.class, e.getClass());
    }
    // issue unbind on correct reference where bind was actually called
    serverStream.close();
    assertEquals(address, serverStream.listenUnbind().toBlocking().toFuture().get());
  }

  @Test
  public void testServerStreamBindsManyTimes() throws Exception {
    ListeningServerStream serverStream0 = templateServerStream;
    ListeningServerStream serverStream1 = serverStream0.bind();
    ListeningServerStream serverStream2 = serverStream0.bind();
    ListeningServerStream serverStream3 = serverStream0.bind();
    try {
      assertThat(serverStream0, not(sameInstance(serverStream1)));
      assertThat(serverStream1, not(sameInstance(serverStream2)));
      assertThat(serverStream2, not(sameInstance(serverStream3)));
    } finally {
      serverStream0.close();
      serverStream1.close();
      serverStream2.close();
      serverStream3.close();
    }
  }

  @Test
  public void testServerStreamBindsOnAvailablePort() throws Exception {
    int startPort = 3801;
    ListeningServerStream serverStream1 = templateServerStream.withPort(startPort).bind();
    ListeningServerStream serverStream2 = templateServerStream.withPort(startPort).bind();
    try {
      assertEquals(Address.create("127.0.0.1", 3801), serverStream1.listenBind().toBlocking().toFuture().get());
      assertEquals(Address.create("127.0.0.1", 3802), serverStream2.listenBind().toBlocking().toFuture().get());
    } finally {
      serverStream1.close();
      serverStream2.close();
    }
  }

  @Test
  public void testServerStreamBindsThenUnbinds() throws Exception {
    ListeningServerStream stream1 = templateServerStream.bind();
    try {
      assertEquals(address, stream1.listenBind().toBlocking().toFuture().get());
    } finally {
      stream1.close();
    }
    // After previous successfull (hopefully) close() it's possible to bind again on port
    ListeningServerStream stream2 = templateServerStream.bind();
    try {
      assertEquals(address, stream2.listenBind().toBlocking().toFuture().get());
    } finally {
      stream2.close();
    }
  }

  @Test
  public void testServerStreamListenBindUnbindAfterClose() throws Exception {
    ListeningServerStream serverStream = templateServerStream.bind();
    // check we received address on which we were bound
    assertEquals(address, serverStream.listenBind().toBlocking().toFuture().get());
    // .. and again
    assertEquals(address, serverStream.listenBind().toBlocking().toFuture().get());

    serverStream.close();
    // check we received address on which we were unbound
    assertEquals(address, serverStream.listenUnbind().toBlocking().toFuture().get());
    // .. and again
    assertEquals(address, serverStream.listenUnbind().toBlocking().toFuture().get());
    // plus we can listen to bind even after server stream close
    assertEquals(address, serverStream.listenBind().toBlocking().toFuture().get());
  }

  @Test
  public void testServerStreamOnClose() throws Exception {
    ListeningServerStream serverStream = templateServerStream.bind();
    AtomicBoolean onCloseBoolean = new AtomicBoolean();
    serverStream.listenClose(aVoid -> onCloseBoolean.set(true));

    Address address = serverStream.listenBind().toBlocking().toFuture().get();

    ClientStream clientStream = ClientStream.newClientStream();
    clientStream.send(address, ServiceMessage.withQualifier("q/test").build());

    Subject<Event, Event> emittedEventsSubject = ReplaySubject.create();
    clientStream.listen().subscribe(emittedEventsSubject);

    List<Event> events = emittedEventsSubject.buffer(2).timeout(3, TimeUnit.SECONDS).toBlocking().first();
    assertEquals(2, events.size());
    assertEquals(Topic.MessageWrite, events.get(0).getTopic());
    assertEquals(Topic.WriteSuccess, events.get(1).getTopic());

    serverStream.close();

    assertTrue(onCloseBoolean.get());
  }
}
