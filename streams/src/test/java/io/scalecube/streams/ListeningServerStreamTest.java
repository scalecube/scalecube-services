package io.scalecube.streams;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.scalecube.streams.Event.Topic;
import io.scalecube.transport.Address;

import org.junit.Before;
import org.junit.Test;

import rx.observers.AssertableSubscriber;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ListeningServerStreamTest {

  private static final long TIMEOUT_MILLIS = Duration.ofMillis(3000).toMillis();

  private ListeningServerStream serverStream;

  @Before
  public void setUp() {
    serverStream = ListeningServerStream.newListeningServerStream().withListenAddress("localhost");
  }

  @Test
  public void testBindWithDefaults() {
    ListeningServerStream serverStream =
        ListeningServerStream.newListeningServerStream().withListenAddress("localhost");
    assertEquals("127.0.0.1:5801", serverStream.bindAwait().toString());
  }

  @Test
  public void testServerStreamBindsOnAvailablePort() throws Exception {
    int port = 5555;
    ListeningServerStream listeningServerStream = serverStream.withPort(port);
    Address address1 = listeningServerStream.bindAwait();
    Address address2 = listeningServerStream.bindAwait();
    Address address3 = listeningServerStream.bindAwait();
    assertEquals("127.0.0.1:5555", address1.toString());
    assertEquals("127.0.0.1:5556", address2.toString());
    assertEquals("127.0.0.1:5557", address3.toString());
  }

  @Test
  public void testServerStreamBindsThenUnbinds() throws Exception {
    String expectedAddress = "127.0.0.1:5801";
    ListeningServerStream serverStream =
        ListeningServerStream.newListeningServerStream().withListenAddress("localhost");

    try {
      assertEquals(expectedAddress, serverStream.bindAwait().toString());
    } finally {
      serverStream.close();
    }

    // check you can bind on same port after previous close
    serverStream = ListeningServerStream.newListeningServerStream().withListenAddress("localhost");
    try {
      assertEquals(expectedAddress, serverStream.bindAwait().toString());
    } finally {
      serverStream.close();
    }
  }

  @Test
  public void testServerStreamOnClose() throws Exception {
    AtomicBoolean onCloseBoolean = new AtomicBoolean();
    serverStream.listenClose(aVoid -> onCloseBoolean.set(true));
    serverStream.close();
    assertTrue(onCloseBoolean.get());
  }

  @Test
  public void testBranchingAtBind() {
    int port = 4444;
    ListeningServerStream root = serverStream.withListenAddress("localhost");
    assertEquals("127.0.0.1:4444", root.withPort(port).bindAwait().toString());
    assertEquals("127.0.0.1:4445", root.withPort(port).bindAwait().toString());
  }

  @Test
  public void testBranchingThenUnbind() throws Exception {
    int port = 4801;
    ListeningServerStream root = serverStream.withPort(port).withListenAddress("localhost");
    assertEquals("127.0.0.1:4801", root.bindAwait().toString());
    assertEquals("127.0.0.1:4802", root.bindAwait().toString());
    assertEquals("127.0.0.1:4803", root.bindAwait().toString());
    assertEquals("127.0.0.1:4804", root.bindAwait().toString());

    // now unbind
    root.close();

    // await a bit
    TimeUnit.SECONDS.sleep(3);

    // ensure we can bind again because close() cleared server channels
    assertEquals("127.0.0.1:4801", root.bindAwait().toString());
  }

  @Test
  public void testServerStreamRemotePartyClosed() throws Exception {
    AssertableSubscriber<Event> serverStreamSubscriber = serverStream.listen().test();

    Address address = serverStream.bindAwait();

    ClientStream clientStream = ClientStream.newClientStream();
    clientStream.send(address, StreamMessage.builder().qualifier("q/test").build());

    List<Event> events =
        serverStreamSubscriber.awaitValueCount(2, TIMEOUT_MILLIS, TimeUnit.MILLISECONDS).getOnNextEvents();
    assertEquals(Topic.ChannelContextSubscribed, events.get(0).getTopic());
    assertEquals(Topic.ReadSuccess, events.get(1).getTopic());

    // close remote party and receive corresp events
    AssertableSubscriber<Event> channelInactiveSubscriber =
        serverStream.listenChannelContextClosed().test();

    // close connector channel at client stream
    clientStream.close();

    // await a bit
    TimeUnit.SECONDS.sleep(3);

    // assert that serverStream received event about closed client connector channel
    Event event = channelInactiveSubscriber.getOnNextEvents().get(0);
    assertEquals(Topic.ChannelContextClosed, event.getTopic());
    assertFalse("Must not have error at this point", event.hasError());
  }
}
