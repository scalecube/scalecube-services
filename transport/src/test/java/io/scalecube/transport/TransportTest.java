package io.scalecube.transport;

import static com.google.common.base.Throwables.propagate;
import static org.junit.Assert.*;

import io.scalecube.testlib.BaseTest;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.netty.channel.ConnectTimeoutException;

import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Subscriber;
import rx.functions.Action1;

import java.net.ConnectException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@SuppressWarnings("unchecked")
public class TransportTest extends BaseTest {
  static final Logger LOGGER = LoggerFactory.getLogger(TransportTest.class);

  Transport client;
  Transport server;

  @After
  public void tearDown() throws Exception {
    destroyTransport(client);
    destroyTransport(server);
  }

  @Test
  public void testInteractWithNoConnection() throws Exception {
    TransportEndpoint serverEndpoint = serverEndpoint();
    for (int i = 0; i < 10; i++) {
      LOGGER.info("####### {} : iteration = {}", testName.getMethodName(), i);

      client = createTransport(clientEndpoint());

      // create transport and don't wait just send message
      SettableFuture<Void> sendPromise0 = SettableFuture.create();
      client.send(serverEndpoint, new Message("q"), sendPromise0);
      try {
        sendPromise0.get(3, TimeUnit.SECONDS);
        fail();
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        assertNotNull(cause);
        assertAmongExpectedClasses(cause.getClass(),
            ClosedChannelException.class, ConnectException.class, ConnectTimeoutException.class);
      }

      // send second message: no connection yet and it's clear that there's no connection
      SettableFuture<Void> sendPromise1 = SettableFuture.create();
      client.send(serverEndpoint, new Message("q"), sendPromise1);
      try {
        sendPromise1.get(3, TimeUnit.SECONDS);
        fail();
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        assertNotNull(cause);
        assertAmongExpectedClasses(cause.getClass(),
            ClosedChannelException.class, ConnectException.class, ConnectTimeoutException.class);
      }

      destroyTransport(client);
    }
  }

  @Test(expected = TransportClosedException.class)
  public void testDisconnectAndSendSequentiallyFail() throws Throwable {
    final TransportEndpoint clientEndpoint = clientEndpoint();
    final TransportEndpoint serverEndpoint = serverEndpoint();

    client = createTransport(clientEndpoint);
    server = createTransport(serverEndpoint);

    for (int i = 0; i < 100; i++) {
      LOGGER.info("####### {} : iteration = {}", testName.getMethodName(), i);

      // Send message
      SettableFuture<Void> sentPromise = SettableFuture.create();
      client.send(serverEndpoint, new Message("Hello 0 at #" + i), sentPromise);

      // Disconnect without waiting for message sent
      SettableFuture<Void> disconnectedPromise = SettableFuture.create();
      client.disconnect(serverEndpoint, disconnectedPromise);

      // Eventually send should fail with TransportClosedException since either async disconnect will close channel
      // before
      // message sent or next sent in a loop will be allocated on the channel which is disconnecting instead of creating
      // new connection.
      try {
        sentPromise.get(1, TimeUnit.SECONDS);
      } catch (InterruptedException | TimeoutException | ExecutionException e) {
        throw e.getCause();
      }
    }
  }

  @Test
  public void testDisconnectAndSendSequentiallySuccess() throws Exception {
    final TransportEndpoint clientEndpoint = clientEndpoint();
    final TransportEndpoint serverEndpoint = serverEndpoint();

    client = createTransport(clientEndpoint);
    server = createTransport(serverEndpoint);

    for (int i = 0; i < 10; i++) {
      LOGGER.info("####### {} : iteration = {}", testName.getMethodName(), i);

      // Connect and send
      SettableFuture<Void> sentPromise = SettableFuture.create();
      client.send(serverEndpoint, new Message("Hello 0 at #" + i), sentPromise);

      // Wait that message was sent
      sentPromise.get(1, TimeUnit.SECONDS);

      // Disconnect
      SettableFuture<Void> disconnectedPromise = SettableFuture.create();
      client.disconnect(serverEndpoint, disconnectedPromise);

      // Wait disconnected
      disconnectedPromise.get(1, TimeUnit.SECONDS);

      // TODO [AK]: Behavior described below is understandable, but undesirable. Need to think about improvement here.
      // Wait some time until disconnection is detected on a server side and related accepted channel is cleared
      // otherwise following connect may fail on server with:
      // i.s.t.TransportBrokenException: Detected duplicate TransportChannel{...} for key=... in accepted_map
      pause(100);
    }
  }

  @Test
  public void testPingPongClientTFListenAndServerTFListen() throws Exception {
    final TransportEndpoint clientEndpoint = clientEndpoint();
    final TransportEndpoint serverEndpoint = serverEndpoint();

    client = createTransport(clientEndpoint);
    server = createTransport(serverEndpoint);

    server.listen().subscribe(new Action1<Message>() {
      @Override
      public void call(Message message) {
        TransportEndpoint endpoint = message.sender();
        assertEquals("Expected clientEndpoint", clientEndpoint, endpoint);
        send(server, endpoint, new Message(null, TransportHeaders.QUALIFIER, "hi client"));
      }
    });

    // final ValueLatch<Message> latch = new ValueLatch<>();
    final SettableFuture<Message> messageFuture = SettableFuture.create();
    client.listen().subscribe(new Action1<Message>() {
      @Override
      public void call(Message message) {
        messageFuture.set(message);
      }
    });

    send(client, serverEndpoint, new Message(null, TransportHeaders.QUALIFIER, "hello server"));

    Message result = messageFuture.get(3, TimeUnit.SECONDS);
    assertNotNull("No response from serverEndpoint", result);
    assertEquals("hi client", result.header(TransportHeaders.QUALIFIER));
  }

  @Test
  public void testConnectorSendOrder1Thread() throws Exception {
    TransportEndpoint clientEndpoint = clientEndpoint();
    TransportEndpoint serverEndpoint = serverEndpoint();

    client = createTransport(clientEndpoint, 100);
    server = createTransport(serverEndpoint, 100);

    int total = 1000;
    for (int i = 0; i < 10; i++) {
      LOGGER.info("####### {} : iteration = {}", testName.getMethodName(), i);

      final List<Message> received = new ArrayList<>();
      final CountDownLatch latch = new CountDownLatch(total);
      server.listen().subscribe(new Action1<Message>() {
        @Override
        public void call(Message message) {
          received.add(message);
          latch.countDown();
        }
      });

      for (int j = 0; j < total; j++) {
        SettableFuture<Void> send = SettableFuture.create();
        client.send(serverEndpoint, new Message(null, TransportHeaders.QUALIFIER, "q" + j), send);
        try {
          send.get(3, TimeUnit.SECONDS);
        } catch (Exception e) {
          LOGGER.error("Failed to send message: j = {}", j, e);
          propagate(e);
        }
      }

      latch.await(20, TimeUnit.SECONDS);
      {
        SettableFuture<Void> close = SettableFuture.create();
        client.disconnect(serverEndpoint, close);
        close.get(1, TimeUnit.SECONDS);
      }
      pause(100); // wait a bit so close could recognized on other side

      assertSendOrder(total, received);
    }
  }

  @Test
  public void testConnectorSendOrder4Thread() throws Exception {
    TransportEndpoint clientEndpoint = clientEndpoint(49050);
    TransportEndpoint serverEndpoint = serverEndpoint(49060);

    Transport client = createTransport(clientEndpoint, 100);
    Transport server = createTransport(serverEndpoint, 100);

    final int total = 1000;
    for (int i = 0; i < 10; i++) {
      LOGGER.info("####### {} : iteration = {}", testName.getMethodName(), i);
      ExecutorService exec = Executors.newFixedThreadPool(4, new ThreadFactoryBuilder().setDaemon(true).build());

      final List<Message> received = new ArrayList<>();
      final CountDownLatch latch = new CountDownLatch(4 * total);
      server.listen().subscribe(new Action1<Message>() {
        @Override
        public void call(Message message) {
          received.add(message);
          latch.countDown();
        }
      });

      Future<Void> f0 = exec.submit(sender(0, client, serverEndpoint, total));
      Future<Void> f1 = exec.submit(sender(1, client, serverEndpoint, total));
      Future<Void> f2 = exec.submit(sender(2, client, serverEndpoint, total));
      Future<Void> f3 = exec.submit(sender(3, client, serverEndpoint, total));

      latch.await(20, TimeUnit.SECONDS);

      f0.get(1, TimeUnit.SECONDS);
      f1.get(1, TimeUnit.SECONDS);
      f2.get(1, TimeUnit.SECONDS);
      f3.get(1, TimeUnit.SECONDS);

      {
        SettableFuture<Void> close = SettableFuture.create();
        client.disconnect(serverEndpoint, close);
        close.get(1, TimeUnit.SECONDS);
      }
      pause(100); // wait a bit so close could recognized on other side
      exec.shutdownNow();

      assertSenderOrder(0, total, received);
      assertSenderOrder(1, total, received);
      assertSenderOrder(2, total, received);
      assertSenderOrder(3, total, received);
    }

    destroyTransport(client);
    destroyTransport(server);
  }

  @Test
  public void testNetworkSettings() throws InterruptedException {
    TransportEndpoint clientEndpoint = clientEndpoint();
    TransportEndpoint serverEndpoint = serverEndpoint();

    client = createTransport(clientEndpoint);
    server = createTransport(serverEndpoint);

    int lostPercent = 50;
    int mean = 0;
    client.<TransportPipelineFactory>getPipelineFactory().setNetworkSettings(serverEndpoint, lostPercent, mean);

    final List<Message> serverMessageList = new ArrayList<>();
    server.listen().subscribe(new Action1<Message>() {
      @Override
      public void call(Message message) {
        serverMessageList.add(message);
      }
    });

    int total = 1000;
    for (int i = 0; i < total; i++) {
      client.send(serverEndpoint, new Message("q" + i), null);
    }

    pause(1000);

    int expectedMax = total / 100 * lostPercent + total / 100 * 5; // +5% for maximum possible lost messages
    int size = serverMessageList.size();
    assertTrue("expectedMax=" + expectedMax + ", actual size=" + size, size < expectedMax);
  }

  @Test
  public void testPingPongOnSingleChannel() throws Exception {
    TransportEndpoint clientEndpoint = clientEndpoint();
    TransportEndpoint serverEndpoint = serverEndpoint();

    server = createTransport(serverEndpoint);
    client = createTransport(clientEndpoint);

    server.listen().buffer(2).subscribe(new Action1<List<Message>>() {
      @Override
      public void call(List<Message> messages) {
        for (Message message : messages) {
          Message echo = new Message("echo/" + message.header(TransportHeaders.QUALIFIER));
          server.send(message.sender(), echo, null);
        }
      }
    });

    final SettableFuture<List<Message>> targetFuture = SettableFuture.create();
    client.listen().buffer(2).subscribe(new Action1<List<Message>>() {
      @Override
      public void call(List<Message> messages) {
        targetFuture.set(messages);
      }
    });

    client.send(serverEndpoint, new Message("q1"), null);
    client.send(serverEndpoint, new Message("q2"), null);

    List<Message> target = targetFuture.get(1, TimeUnit.SECONDS);
    assertNotNull(target);
    assertEquals(2, target.size());
  }

  @Test
  public void testPingPongOnSeparateChannel() throws Exception {
    TransportEndpoint clientEndpoint = clientEndpoint();
    TransportEndpoint serverEndpoint = serverEndpoint();

    server = createTransport(serverEndpoint);
    client = createTransport(clientEndpoint);

    server.listen().buffer(2).subscribe(new Action1<List<Message>>() {
      @Override
      public void call(List<Message> messages) {
        for (Message message : messages) {
          Message echo = new Message("echo/" + message.header(TransportHeaders.QUALIFIER));
          server.send(message.sender(), echo, null);
        }
      }
    });

    final SettableFuture<List<Message>> targetFuture = SettableFuture.create();
    client.listen().buffer(2).subscribe(new Action1<List<Message>>() {
      @Override
      public void call(List<Message> messages) {
        targetFuture.set(messages);
      }
    });

    client.send(serverEndpoint, new Message("q1"), null);
    client.send(serverEndpoint, new Message("q2"), null);

    List<Message> target = targetFuture.get(1, TimeUnit.SECONDS);
    assertNotNull(target);
    assertEquals(2, target.size());
  }

  @Test
  public void testCompleteObserver() throws Exception {
    TransportEndpoint clientEndpoint = clientEndpoint();
    TransportEndpoint serverEndpoint = serverEndpoint();

    server = createTransport(serverEndpoint);
    client = createTransport(clientEndpoint);

    final SettableFuture<Boolean> completeLatch = SettableFuture.create();
    final SettableFuture<Message> messageLatch = SettableFuture.create();

    server.listen().subscribe(new Subscriber<Message>() {
      @Override
      public void onCompleted() {
        completeLatch.set(true);
      }

      @Override
      public void onError(Throwable e) {}

      @Override
      public void onNext(Message message) {
        messageLatch.set(message);
      }
    });

    SettableFuture<Void> send = SettableFuture.create();
    client.send(serverEndpoint, new Message("q"), send);
    send.get(1, TimeUnit.SECONDS);

    assertNotNull(messageLatch.get(1, TimeUnit.SECONDS));

    SettableFuture<Void> close = SettableFuture.create();
    server.stop(close);
    close.get();

    assertTrue(completeLatch.get(1, TimeUnit.SECONDS));
  }

  @Test
  public void testObserverThrowsException() throws Exception {
    TransportEndpoint clientEndpoint = clientEndpoint();
    TransportEndpoint serverEndpoint = serverEndpoint();

    server = createTransport(serverEndpoint);
    client = createTransport(clientEndpoint);

    server.listen().subscribe(new Action1<Message>() {
      @Override
      public void call(Message message) {
        String qualifier = message.data();
        if (qualifier.startsWith("throw")) {
          throw new RuntimeException("" + message);
        }
        if (qualifier.startsWith("q")) {
          Message echo = new Message("echo/" + message.header(TransportHeaders.QUALIFIER));
          server.send(message.sender(), echo);
        }
      }
    }, new Action1<Throwable>() {
      @Override
      public void call(Throwable throwable) {
        throwable.printStackTrace();
      }
    });

    // send "throw" and raise exception on server subscriber
    final SettableFuture<Message> messageFuture0 = SettableFuture.create();
    client.listen().subscribe(new Action1<Message>() {
      @Override
      public void call(Message message) {
        messageFuture0.set(message);
      }
    });
    client.send(serverEndpoint, new Message("throw"), null);
    Message message0 = null;
    try {
      message0 = messageFuture0.get(1, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      // ignore since expected behavior
    }
    assertNull(message0);

    // send normal message and check whether server subscriber is broken (no response)
    final SettableFuture<Message> messageFuture1 = SettableFuture.create();
    client.listen().subscribe(new Action1<Message>() {
      @Override
      public void call(Message message) {
        messageFuture1.set(message);
      }
    });
    client.send(serverEndpoint, new Message("q"), null);
    Message transportMessage1 = null;
    try {
      transportMessage1 = messageFuture1.get(1, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      // ignore since expected behavior
    }
    assertNull(transportMessage1);
  }

  @Test
  public void testBlockAndUnblockTraffic() throws Exception {
    TransportEndpoint clientEndpoint = clientEndpoint();
    TransportEndpoint serverEndpoint = serverEndpoint();

    client = createTransport(clientEndpoint);
    server = createTransport(serverEndpoint);

    server.listen().subscribe(new Action1<Message>() {
      @Override
      public void call(Message message) {
        server.send(message.sender(), message);
      }
    });

    final List<Message> resp = new ArrayList<>();
    client.listen().subscribe(new Action1<Message>() {
      @Override
      public void call(Message message) {
        resp.add(message);
      }
    });

    // test at unblocked transport
    send(client, serverEndpoint, new Message(null, TransportHeaders.QUALIFIER, "q/unblocked"));

    // then block client->server messages
    pause(1000);
    client.<TransportPipelineFactory>getPipelineFactory().blockMessagesTo(serverEndpoint);
    send(client, serverEndpoint, new Message(null, TransportHeaders.QUALIFIER, "q/blocked"));

    pause(1000);
    assertEquals(1, resp.size());
    assertEquals("q/unblocked", resp.get(0).header(TransportHeaders.QUALIFIER));
  }

  @Test
  public void testSendMailboxBecomingFull() throws Exception {
    TransportEndpoint clientEndpoint = clientEndpoint();
    TransportEndpoint serverEndpoint = serverEndpoint();

    client = createTransport(clientEndpoint, 1);
    server = createTransport(serverEndpoint, 1);

    client.send(serverEndpoint, new Message(null, TransportHeaders.QUALIFIER, "ping0"), null);

    SettableFuture<Void> send1 = SettableFuture.create();
    client.send(serverEndpoint, new Message(null, TransportHeaders.QUALIFIER, "ping1"), send1);
    try {
      send1.get(1, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      TransportMessageException cause = (TransportMessageException) e.getCause();
      assertNotNull(cause);
    }
  }

  private TransportEndpoint serverEndpoint() {
    return TransportEndpoint.from("localhost:49255:server");
  }

  private TransportEndpoint clientEndpoint() {
    return TransportEndpoint.from("localhost:49355:client");
  }

  private TransportEndpoint serverEndpoint(int port) {
    return TransportEndpoint.from("localhost:" + port + ":" + server);
  }

  private TransportEndpoint clientEndpoint(int port) {
    return TransportEndpoint.from("localhost:" + port + ":" + client);
  }

  private void pause(int millis) throws InterruptedException {
    Thread.sleep(millis);
  }

  private void assertSendOrder(int total, List<Message> received) {
    ArrayList<Message> messages = new ArrayList<>(received);
    assertEquals(total, messages.size());
    for (int k = 0; k < total; k++) {
      assertEquals("q" + k, messages.get(k).header(TransportHeaders.QUALIFIER));
    }
  }

  private Callable<Void> sender(final int id, final Transport client, final TransportEndpoint endpoint, final int total) {
    return new Callable<Void>() {
      public Void call() throws Exception {
        for (int j = 0; j < total; j++) {
          String correlationId = id + "/" + j;
          SettableFuture<Void> sendPromise = SettableFuture.create();
          client.send(endpoint, new Message(null, TransportHeaders.QUALIFIER, "q", TransportHeaders.CORRELATION_ID,
              correlationId), sendPromise);
          try {
            sendPromise.get(3, TimeUnit.SECONDS);
          } catch (Exception e) {
            LOGGER.error("Failed to send message: j = {} id = {}", j, id, e);
            propagate(e);
          }
        }
        return null;
      }
    };
  }

  private void assertSenderOrder(int id, int total, List<Message> received) {
    ArrayList<Message> messages = new ArrayList<>(received);
    ArrayListMultimap<Integer, Message> group = ArrayListMultimap.create();
    for (Message message : messages) {
      group.put(Integer.valueOf(message.header(TransportHeaders.CORRELATION_ID).split("/")[0]), message);
    }
    assertEquals(total, group.get(id).size());
    for (int k = 0; k < total; k++) {
      assertEquals(id + "/" + k, group.get(id).get(k).header(TransportHeaders.CORRELATION_ID));
    }
  }

  private void send(final ITransport from, final TransportEndpoint to, final Message msg) {
    final SettableFuture<Void> f = SettableFuture.create();
    f.addListener(new Runnable() {
      @Override
      public void run() {
        if (f.isDone()) {
          try {
            f.get();
          } catch (Exception e) {
            LOGGER.error("Failed to send {} to {} from transport: {}, cause: {}", msg, to, from, e.getCause());
          }
        }
      }
    }, MoreExecutors.directExecutor());
    from.send(to, msg, f);
  }

  private Transport createTransport(TransportEndpoint endpoint) {
    return createTransport(endpoint, 1000);
  }

  private Transport createTransport(TransportEndpoint endpoint, int sendHwm) {
    Transport transport =
        Transport.newInstance(endpoint, TransportSettings.builder().connectTimeout(1000).sendHighWaterMark(sendHwm)
            .useNetworkEmulator(true).build());
    try {
      transport.start().get();
    } catch (Exception e) {
      LOGGER.error("Failed to start transport ", e);
    }

    return transport;
  }

  private void destroyTransport(Transport transport) throws Exception {
    if (transport != null) {
      SettableFuture<Void> close = SettableFuture.create();
      transport.stop(close);
      close.get(1, TimeUnit.SECONDS);
    }
  }

}
