package io.scalecube.ipc;

import static io.scalecube.ipc.Event.Topic;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.scalecube.transport.Address;

import org.junit.Before;
import org.junit.Test;

import rx.observers.AssertableSubscriber;
import rx.subjects.BehaviorSubject;
import rx.subjects.Subject;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ListeningServerStreamTest {

  private static final Duration TIMEOUT = Duration.ofMillis(3000);
  private static final long TIMEOUT_MILLIS = TIMEOUT.toMillis();

  private Address address;
  private ListeningServerStream serverStreamTemplate;

  @Before
  public void setUp() {
    int port = 5801;
    address = Address.create("127.0.0.1", port);
    serverStreamTemplate = ListeningServerStream.newServerStream().withListenAddress("localhost").withPort(port);
  }

  @Test
  public void testServerStreamListenBindNotReturnIfBindNotCalled() throws Exception {
    // issue server stream bind
    ListeningServerStream serverStream = serverStreamTemplate.bind();
    try {
      // check bind on wrong reference
      try {
        serverStreamTemplate.listenBind().toBlocking().toFuture().get(1, TimeUnit.SECONDS);
        fail("Expected TimeoutException");
      } catch (Exception e) {
        assertEquals(TimeoutException.class, e.getClass());
      }
      // check stream is bound on correct reference
      assertEquals(address, serverStream.bindAwait());
    } finally {
      serverStream.close();
      assertEquals(address, serverStream.unbindAwait());
    }
  }

  @Test
  public void testServerStreamListenUnbindNotReturnIfCloseNotCalled() throws Exception {
    // check stream is bound
    ListeningServerStream serverStream = serverStreamTemplate.bind();
    try {
      assertEquals(address, serverStream.bindAwait());
      // issue close on reference which is not bound
      serverStreamTemplate.close();
      try {
        serverStreamTemplate.listenUnbind().toBlocking().toFuture().get(1, TimeUnit.SECONDS);
        fail("Expected TimeoutException");
      } catch (Exception e) {
        assertEquals(TimeoutException.class, e.getClass());
      }
    } finally {
      serverStream.close();
      assertEquals(address, serverStream.unbindAwait());
    }
  }

  @Test
  public void testServerStreamBindsManyTimes() throws Exception {
    ListeningServerStream serverStream0 = serverStreamTemplate;
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
    ListeningServerStream serverStream1 = serverStreamTemplate.withPort(startPort).bind();
    ListeningServerStream serverStream2 = serverStreamTemplate.withPort(startPort).bind();
    try {
      assertEquals(Address.create("127.0.0.1", 3801), serverStream1.bindAwait());
      assertEquals(Address.create("127.0.0.1", 3802), serverStream2.bindAwait());
    } finally {
      serverStream1.close();
      serverStream2.close();
    }
  }

  @Test
  public void testServerStreamBindsThenUnbinds() throws Exception {
    ListeningServerStream stream1 = serverStreamTemplate.bind();
    try {
      assertEquals(address, stream1.bindAwait());
    } finally {
      stream1.close();
    }
    // After previous successfull (hopefully) close() it's possible to bind again on port
    ListeningServerStream stream2 = serverStreamTemplate.bind();
    try {
      assertEquals(address, stream2.bindAwait());
    } finally {
      stream2.close();
    }
  }

  @Test
  public void testServerStreamOnClose() throws Exception {
    ListeningServerStream serverStream = serverStreamTemplate.bind();
    Address address = serverStream.bindAwait();
    try {
      AtomicBoolean onCloseBoolean = new AtomicBoolean();
      serverStream.listenClose(aVoid -> onCloseBoolean.set(true));
      serverStream.close();
      assertTrue(onCloseBoolean.get());
    } finally {
      assertEquals(address, serverStream.unbindAwait());
    }
  }

  @Test
  public void testServerStreamRemotePartyClosed() throws Exception {
    ListeningServerStream serverStream = serverStreamTemplate.bind();
    Address address = serverStream.bindAwait();

    ClientStream clientStream = ClientStream.newClientStream();
    clientStream.send(address, ServiceMessage.withQualifier("q/test").build());

    Subject<Event, Event> serverStreamSubject = BehaviorSubject.create();
    serverStream.listen().subscribe(serverStreamSubject);
    AssertableSubscriber<Event> serverStreamSubscriber = serverStreamSubject.test();

    List<Event> events =
        serverStreamSubscriber.awaitValueCount(2, TIMEOUT_MILLIS, TimeUnit.MILLISECONDS).getOnNextEvents();
    assertEquals(Topic.ChannelContextSubscribed, events.get(0).getTopic());
    assertEquals(Topic.ReadSuccess, events.get(1).getTopic());

    // close remote party and receive corresp events
    BehaviorSubject<Event> channelInactiveSubject = BehaviorSubject.create();
    serverStream.listenChannelContextClosed().subscribe(channelInactiveSubject);

    // close connector channel at client stream
    clientStream.close();

    // await a bit
    TimeUnit.SECONDS.sleep(3);

    // assert that serverStream received event about closed client connector channel
    Event event = channelInactiveSubject.test().getOnNextEvents().get(0);
    assertEquals(Topic.ChannelContextClosed, event.getTopic());
    assertFalse("Must not have error at this point", event.hasError());
  }
}
