package io.scalecube.transport;

import static io.scalecube.transport.TransportTestUtils.createTransport;
import static io.scalecube.transport.TransportTestUtils.destroyTransport;
import static io.scalecube.transport.TransportTestUtils.send;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.scalecube.testlib.BaseTest;

import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Subscriber;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TransportTest extends BaseTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransportTest.class);

  // Auto-destroyed on tear down
  private Transport client;
  private Transport server;

  @After
  public void tearDown() throws Exception {
    destroyTransport(client);
    destroyTransport(server);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidListenConfig() {
    Transport transport = null;
    try {
      TransportConfig config = TransportConfig.builder().listenInterface("eth0").listenAddress("10.10.10.10").build();
      transport = Transport.bindAwait(config);
    } finally {
      destroyTransport(transport);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidListenInterface() {
    Transport transport = null;
    try {
      TransportConfig config = TransportConfig.builder().listenInterface("yadayada").build();
      transport = Transport.bindAwait(config);
    } finally {
      destroyTransport(transport);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidListenAddress() {
    Transport transport = null;
    try {
      TransportConfig config = TransportConfig.builder().listenAddress("0.0.0.0").build();
      transport = Transport.bindAwait(config);
    } finally {
      destroyTransport(transport);
    }
  }

  @Test
  public void testPortAutoIncrementRaceConditions() throws Exception {
    int count = 30;
    TransportConfig config = TransportConfig.builder()
        .port(6000)
        .portAutoIncrement(true)
        .portCount(count)
        .build();

    Map<CompletableFuture<Transport>, Boolean> transports = new ConcurrentHashMap<>();
    ExecutorService executor = Executors.newFixedThreadPool(4);
    for (int i = 0; i < count; i++) {
      executor.execute(() -> transports.put(Transport.bind(config), true));
    }
    executor.shutdown();
    executor.awaitTermination(60, TimeUnit.SECONDS);

    CompletableFuture<Void> allFuturesResult =
        CompletableFuture.allOf(transports.keySet().toArray(new CompletableFuture[transports.size()]));

    // Destroy transports
    try {
      allFuturesResult.get(60, TimeUnit.SECONDS);
    } finally {
      for (CompletableFuture<Transport> transportFuture : transports.keySet()) {
        if (transportFuture.isDone()) {
          destroyTransport(transportFuture.get());
        }
      }
    }
  }

  @Test
  public void testBindExceptionWithoutPortAutoIncrement() throws Exception {
    TransportConfig config = TransportConfig.builder()
        .port(6000)
        .portAutoIncrement(false)
        .portCount(100)
        .build();
    Transport transport1 = null;
    Transport transport2 = null;
    try {
      transport1 = Transport.bindAwait(config);
      transport2 = Transport.bindAwait(config);
      fail("Didn't get expected bind exception");
    } catch (Throwable throwable) {
      // Check that get address already in use exception
      assertTrue(throwable instanceof BindException || throwable.getMessage().contains("Address already in use"));
    } finally {
      destroyTransport(transport1);
      destroyTransport(transport2);
    }
  }

  @Test
  public void testNoBindExceptionWithPortAutoIncrement() throws Exception {
    TransportConfig config = TransportConfig.builder()
        .port(6000)
        .portAutoIncrement(true)
        .portCount(100)
        .build();
    Transport transport1 = null;
    Transport transport2 = null;

    try {
      transport1 = Transport.bindAwait(config);
      transport2 = Transport.bindAwait(config);
    } finally {
      destroyTransport(transport1);
      destroyTransport(transport2);
    }
  }

  @Test
  public void testNoBindExceptionWithPortAutoIncrementWithHalfClosedSocket() throws Exception {
    // Create half-closed socket scenario setup: server socket, connecting client socket, accepted socket
    // on server side being closed, connected socket doesn't react on server's close

    ServerSocket serverSocket = new ServerSocket(6000);
    Thread acceptor = new Thread(() -> {
      while (true) {
        Socket accepted = null;
        try {
          accepted = serverSocket.accept();
          accepted.close();
        } catch (Exception ignore) {
        }
      }
    });
    acceptor.setDaemon(true);
    acceptor.start();

    Socket socket = new Socket(serverSocket.getInetAddress(), serverSocket.getLocalPort());

    // Pretend that this port was chosen as bind port for the transport
    int transportBindPort = socket.getLocalPort();

    TransportConfig config = TransportConfig.builder()
        .port(transportBindPort)
        .portAutoIncrement(true)
        .portCount(100)
        .build();
    Transport transport1 = null;
    try {
      transport1 = Transport.bindAwait(config);
    } finally {
      destroyTransport(transport1);
      serverSocket.close();
      socket.close();
    }
  }

  @Test
  public void testValidListenAddress() {
    Transport transport = null;
    try {
      TransportConfig config = TransportConfig.builder().listenAddress("127.0.0.1").build();
      transport = Transport.bindAwait(config);
    } finally {
      destroyTransport(transport);
    }
  }

  @Test
  public void testUnresolvedHostConnection() throws Exception {
    client = createTransport();
    // create transport with wrong host
    CompletableFuture<Void> sendPromise0 = new CompletableFuture<>();
    client.send(Address.from("wronghost:49255"), Message.fromData("q"), sendPromise0);
    try {
      sendPromise0.get(5, TimeUnit.SECONDS);
      fail();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      assertNotNull(cause);
      assertEquals("Unexpected exception class", UnknownHostException.class, cause.getClass());
    }
  }

  @Test
  public void testInteractWithNoConnection() throws Exception {
    Address serverAddress = Address.from("localhost:49255");
    for (int i = 0; i < 10; i++) {
      LOGGER.info("####### {} : iteration = {}", testName.getMethodName(), i);

      client = createTransport();

      // create transport and don't wait just send message
      CompletableFuture<Void> sendPromise0 = new CompletableFuture<>();
      client.send(serverAddress, Message.fromData("q"), sendPromise0);
      try {
        sendPromise0.get(3, TimeUnit.SECONDS);
        fail();
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        assertNotNull(cause);
        assertTrue("Unexpected exception type (expects IOException)", cause instanceof IOException);
      }

      // send second message: no connection yet and it's clear that there's no connection
      CompletableFuture<Void> sendPromise1 = new CompletableFuture<>();
      client.send(serverAddress, Message.fromData("q"), sendPromise1);
      try {
        sendPromise1.get(3, TimeUnit.SECONDS);
        fail();
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        assertNotNull(cause);
        assertTrue("Unexpected exception type (expects IOException)", cause instanceof IOException);
      }

      destroyTransport(client);
    }
  }

  @Test
  public void testPingPongClientTFListenAndServerTFListen() throws Exception {
    client = createTransport();
    server = createTransport();

    server.listen().subscribe(message -> {
      Address address = message.sender();
      assertEquals("Expected clientAddress", client.address(), address);
      send(server, address, Message.fromQualifier("hi client"));
    });

    CompletableFuture<Message> messageFuture = new CompletableFuture<>();
    client.listen().subscribe(messageFuture::complete);

    send(client, server.address(), Message.fromQualifier("hello server"));

    Message result = messageFuture.get(3, TimeUnit.SECONDS);
    assertNotNull("No response from serverAddress", result);
    assertEquals("hi client", result.qualifier());
  }

  @Test
  public void testNetworkSettings() throws InterruptedException {
    client = createTransport();
    server = createTransport();

    int lostPercent = 50;
    int mean = 0;
    client.networkEmulator().setLinkSettings(server.address(), lostPercent, mean);

    final List<Message> serverMessageList = new ArrayList<>();
    server.listen().subscribe(serverMessageList::add);

    int total = 1000;
    for (int i = 0; i < total; i++) {
      client.send(server.address(), Message.fromData("q" + i));
    }

    Thread.sleep(1000);

    int expectedMax = total / 100 * lostPercent + total / 100 * 5; // +5% for maximum possible lost messages
    int size = serverMessageList.size();
    assertTrue("expectedMax=" + expectedMax + ", actual size=" + size, size < expectedMax);
  }

  @Test
  public void testPingPongOnSingleChannel() throws Exception {
    server = createTransport();
    client = createTransport();

    server.listen().buffer(2).subscribe(messages -> {
      for (Message message : messages) {
        Message echo = Message.fromData("echo/" + message.qualifier());
        server.send(message.sender(), echo);
      }
    });

    final CompletableFuture<List<Message>> targetFuture = new CompletableFuture<>();
    client.listen().buffer(2).subscribe(targetFuture::complete);

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

    server.listen().buffer(2).subscribe(messages -> {
      for (Message message : messages) {
        Message echo = Message.fromData("echo/" + message.qualifier());
        server.send(message.sender(), echo);
      }
    });

    final CompletableFuture<List<Message>> targetFuture = new CompletableFuture<>();
    client.listen().buffer(2).subscribe(targetFuture::complete);

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

    final CompletableFuture<Boolean> completeLatch = new CompletableFuture<>();
    final CompletableFuture<Message> messageLatch = new CompletableFuture<>();

    server.listen().subscribe(new Subscriber<Message>() {
      @Override
      public void onCompleted() {
        completeLatch.complete(true);
      }

      @Override
      public void onError(Throwable e) {}

      @Override
      public void onNext(Message message) {
        messageLatch.complete(message);
      }
    });

    CompletableFuture<Void> send = new CompletableFuture<>();
    client.send(server.address(), Message.fromData("q"), send);
    send.get(1, TimeUnit.SECONDS);

    assertNotNull(messageLatch.get(1, TimeUnit.SECONDS));

    CompletableFuture<Void> close = new CompletableFuture<>();
    server.stop(close);
    close.get();

    assertTrue(completeLatch.get(1, TimeUnit.SECONDS));
  }

  @Test
  public void testObserverThrowsException() throws Exception {
    server = createTransport();
    client = createTransport();

    server.listen().subscribe(message -> {
      String qualifier = message.data();
      if (qualifier.startsWith("throw")) {
        throw new RuntimeException("" + message);
      }
      if (qualifier.startsWith("q")) {
        Message echo = Message.fromData("echo/" + message.qualifier());
        server.send(message.sender(), echo);
      }
    }, Throwable::printStackTrace);

    // send "throw" and raise exception on server subscriber
    final CompletableFuture<Message> messageFuture0 = new CompletableFuture<>();
    client.listen().subscribe(messageFuture0::complete);
    client.send(server.address(), Message.fromData("throw"));
    Message message0 = null;
    try {
      message0 = messageFuture0.get(1, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      // ignore since expected behavior
    }
    assertNull(message0);

    // send normal message and check whether server subscriber is broken (no response)
    final CompletableFuture<Message> messageFuture1 = new CompletableFuture<>();
    client.listen().subscribe(messageFuture1::complete);
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

    server.listen().subscribe(message -> server.send(message.sender(), message));

    final List<Message> resp = new ArrayList<>();
    client.listen().subscribe(resp::add);

    // test at unblocked transport
    send(client, server.address(), Message.fromQualifier("q/unblocked"));

    // then block client->server messages
    Thread.sleep(1000);
    client.networkEmulator().block(server.address());
    send(client, server.address(), Message.fromQualifier("q/blocked"));

    Thread.sleep(1000);
    assertEquals(1, resp.size());
    assertEquals("q/unblocked", resp.get(0).qualifier());
  }

}
