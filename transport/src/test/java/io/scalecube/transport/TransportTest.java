package io.scalecube.transport;

import static com.google.common.base.Throwables.propagate;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
import java.nio.channels.UnresolvedAddressException;
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

  private Transport client;
  private Transport server;

  @After
  public void tearDown() throws Exception {
    destroyTransport(client);
    destroyTransport(server);
  }

  @Test
  public void testUnresolvedHostConnection() throws Exception {
    client = createTransport();
    // create transport with wrong host
    SettableFuture<Void> sendPromise0 = SettableFuture.create();
    client.send(Address.from("wronghost:49255"), Message.fromData("q"), sendPromise0);
    try {
      sendPromise0.get(5, TimeUnit.SECONDS);
      fail();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      assertNotNull(cause);
      assertAmongExpectedClasses(cause.getClass(), UnresolvedAddressException.class);
    }
  }

  @Test
  public void testInteractWithNoConnection() throws Exception {
    Address serverAddress = Address.from("localhost:49255");
    for (int i = 0; i < 10; i++) {
      LOGGER.info("####### {} : iteration = {}", testName.getMethodName(), i);

      client = createTransport();

      // create transport and don't wait just send message
      SettableFuture<Void> sendPromise0 = SettableFuture.create();
      client.send(serverAddress, Message.fromData("q"), sendPromise0);
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
      client.send(serverAddress, Message.fromData("q"), sendPromise1);
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

  @Test
  public void testDisconnectAndSendSequentiallySuccess() throws Exception {
    client = createTransport();
    server = createTransport();

    for (int i = 0; i < 10; i++) {
      LOGGER.info("####### {} : iteration = {}", testName.getMethodName(), i);

      // Connect and send
      SettableFuture<Void> sentPromise = SettableFuture.create();
      client.send(server.address(), Message.fromData("Hello 0 at #" + i), sentPromise);

      // Wait that message was sent
      sentPromise.get(1, TimeUnit.SECONDS);

      // Disconnect
      SettableFuture<Void> disconnectedPromise = SettableFuture.create();
      client.disconnect(server.address(), disconnectedPromise);

      // Wait disconnected
      disconnectedPromise.get(1, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testPingPongClientTFListenAndServerTFListen() throws Exception {
    client = createTransport();
    server = createTransport();

    server.listen().subscribe(new Action1<Message>() {
      @Override
      public void call(Message message) {
        Address address = message.sender();
        assertEquals("Expected clientAddress", client.address(), address);
        send(server, address, Message.fromQualifier("hi client"));
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

    send(client, server.address(), Message.fromQualifier("hello server"));

    Message result = messageFuture.get(3, TimeUnit.SECONDS);
    assertNotNull("No response from serverAddress", result);
    assertEquals("hi client", result.header(MessageHeaders.QUALIFIER));
  }

  @Test
  public void testConnectorSendOrder1Thread() throws Exception {
    client = createTransport();
    server = createTransport();

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
        client.send(server.address(), Message.fromQualifier("q" + j), send);
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
        client.disconnect(server.address(), close);
        close.get(1, TimeUnit.SECONDS);
      }
      pause(100); // wait a bit so close could recognized on other side

      assertSendOrder(total, received);
    }
  }

  @Test
  public void testConnectorSendOrder4Thread() throws Exception {
    Transport client = createTransport();
    Transport server = createTransport();

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

      Future<Void> f0 = exec.submit(sender(0, client, server.address(), total));
      Future<Void> f1 = exec.submit(sender(1, client, server.address(), total));
      Future<Void> f2 = exec.submit(sender(2, client, server.address(), total));
      Future<Void> f3 = exec.submit(sender(3, client, server.address(), total));

      latch.await(20, TimeUnit.SECONDS);

      f0.get(1, TimeUnit.SECONDS);
      f1.get(1, TimeUnit.SECONDS);
      f2.get(1, TimeUnit.SECONDS);
      f3.get(1, TimeUnit.SECONDS);

      {
        SettableFuture<Void> close = SettableFuture.create();
        client.disconnect(server.address(), close);
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
    client = createTransport();
    server = createTransport();

    int lostPercent = 50;
    int mean = 0;
    client.setNetworkSettings(server.address(), lostPercent, mean);

    final List<Message> serverMessageList = new ArrayList<>();
    server.listen().subscribe(new Action1<Message>() {
      @Override
      public void call(Message message) {
        serverMessageList.add(message);
      }
    });

    int total = 1000;
    for (int i = 0; i < total; i++) {
      client.send(server.address(), Message.fromData("q" + i));
    }

    pause(1000);

    int expectedMax = total / 100 * lostPercent + total / 100 * 5; // +5% for maximum possible lost messages
    int size = serverMessageList.size();
    assertTrue("expectedMax=" + expectedMax + ", actual size=" + size, size < expectedMax);
  }

  @Test
  public void testPingPongOnSingleChannel() throws Exception {
    server = createTransport();
    client = createTransport();

    server.listen().buffer(2).subscribe(new Action1<List<Message>>() {
      @Override
      public void call(List<Message> messages) {
        for (Message message : messages) {
          Message echo = Message.fromData("echo/" + message.header(MessageHeaders.QUALIFIER));
          server.send(message.sender(), echo);
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

    client.send(server.address(), Message.fromData("q1"));
    client.send(server.address(), Message.fromData("q2"));

    List<Message> target = targetFuture.get(1, TimeUnit.SECONDS);
    assertNotNull(target);
    assertEquals(2, target.size());
  }

  @Test
  public void testPingPongOnSeparateChannel() throws Exception {
    server = createTransport();
    client = createTransport();

    server.listen().buffer(2).subscribe(new Action1<List<Message>>() {
      @Override
      public void call(List<Message> messages) {
        for (Message message : messages) {
          Message echo = Message.fromData("echo/" + message.header(MessageHeaders.QUALIFIER));
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

    client.send(server.address(), Message.fromData("q1"));
    client.send(server.address(), Message.fromData("q2"));

    List<Message> target = targetFuture.get(1, TimeUnit.SECONDS);
    assertNotNull(target);
    assertEquals(2, target.size());
  }

  @Test
  public void testCompleteObserver() throws Exception {
    server = createTransport();
    client = createTransport();

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
    client.send(server.address(), Message.fromData("q"), send);
    send.get(1, TimeUnit.SECONDS);

    assertNotNull(messageLatch.get(1, TimeUnit.SECONDS));

    SettableFuture<Void> close = SettableFuture.create();
    server.stop(close);
    close.get();

    assertTrue(completeLatch.get(1, TimeUnit.SECONDS));
  }

  @Test
  public void testObserverThrowsException() throws Exception {
    server = createTransport();
    client = createTransport();

    server.listen().subscribe(new Action1<Message>() {
      @Override
      public void call(Message message) {
        String qualifier = message.data();
        if (qualifier.startsWith("throw")) {
          throw new RuntimeException("" + message);
        }
        if (qualifier.startsWith("q")) {
          Message echo = Message.fromData("echo/" + message.header(MessageHeaders.QUALIFIER));
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
    client.send(server.address(), Message.fromData("throw"));
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
    client.send(server.address(), Message.fromData("q"));
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
    client = createTransport();
    server = createTransport();

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
    send(client, server.address(), Message.fromQualifier("q/unblocked"));

    // then block client->server messages
    pause(1000);
    client.blockMessagesTo(server.address());
    send(client, server.address(), Message.fromQualifier("q/blocked"));

    pause(1000);
    assertEquals(1, resp.size());
    assertEquals("q/unblocked", resp.get(0).header(MessageHeaders.QUALIFIER));
  }

  private void pause(int millis) throws InterruptedException {
    Thread.sleep(millis);
  }

  private void assertSendOrder(int total, List<Message> received) {
    ArrayList<Message> messages = new ArrayList<>(received);
    assertEquals(total, messages.size());
    for (int k = 0; k < total; k++) {
      assertEquals("q" + k, messages.get(k).header(MessageHeaders.QUALIFIER));
    }
  }

  private Callable<Void> sender(final int id, final Transport client, final Address address,
      final int total) {
    return new Callable<Void>() {
      public Void call() throws Exception {
        for (int j = 0; j < total; j++) {
          String correlationId = id + "/" + j;
          SettableFuture<Void> sendPromise = SettableFuture.create();
          client.send(address, Message.withQualifier("q").correlationId(correlationId).build(), sendPromise);
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
      group.put(Integer.valueOf(message.correlationId().split("/")[0]), message);
    }
    assertEquals(total, group.get(id).size());
    for (int k = 0; k < total; k++) {
      assertEquals(id + "/" + k, group.get(id).get(k).correlationId());
    }
  }

  private void send(final ITransport from, final Address to, final Message msg) {
    final SettableFuture<Void> sendPromise = SettableFuture.create();
    sendPromise.addListener(new Runnable() {
      @Override
      public void run() {
        if (sendPromise.isDone()) {
          try {
            sendPromise.get();
          } catch (Exception e) {
            LOGGER.error("Failed to send {} to {} from transport: {}, cause: {}", msg, to, from, e.getCause());
          }
        }
      }
    }, MoreExecutors.directExecutor());
    from.send(to, msg, sendPromise);
  }

  private Transport createTransport() {
    TransportConfig config = TransportConfig.builder()
        .connectTimeout(1000)
        .useNetworkEmulator(true)
        .build();
    return Transport.bindAwait(config);
  }

  private void destroyTransport(Transport transport) throws Exception {
    if (transport != null && !transport.isStopped()) {
      SettableFuture<Void> close = SettableFuture.create();
      transport.stop(close);
      close.get(1, TimeUnit.SECONDS);
    }
  }

}
