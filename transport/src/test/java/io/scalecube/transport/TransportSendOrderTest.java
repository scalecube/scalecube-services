package io.scalecube.transport;

import static com.google.common.base.Throwables.propagate;
import static io.scalecube.transport.TransportTestUtils.createTransport;
import static io.scalecube.transport.TransportTestUtils.destroyTransport;
import static org.junit.Assert.assertEquals;

import io.scalecube.testlib.BaseTest;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Subscriber;
import rx.observers.Subscribers;

import java.util.ArrayList;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

/**
 * @author Anton Kharenko
 */
public class TransportSendOrderTest extends BaseTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransportSendOrderTest.class);

  // Auto-destroyed on tear down
  private Transport client;
  private Transport server;

  @After
  public void tearDown() throws Exception {
    destroyTransport(client);
    destroyTransport(server);
  }

  @Test
  public void testSendOrderSingleThreadWithoutPromises() throws Exception {
    server = createTransport();

    int iterationNum = 11; // +1 warm up iteration
    int sentPerIteration = 1000;
    long[] iterationTimeSeries = new long[iterationNum - 1];
    for (int i = 0; i < iterationNum; i++) {
      LOGGER.info("####### {} : iteration = {}", testName.getMethodName(), i);

      client = createTransport();
      final List<Message> received = new ArrayList<>();
      final CountDownLatch latch = new CountDownLatch(sentPerIteration);

      Subscriber<Message> serverSubscriber = Subscribers.create(message -> {
        received.add(message);
        latch.countDown();
      });
      server.listen().subscribe(serverSubscriber);

      long startAt = System.currentTimeMillis();
      for (int j = 0; j < sentPerIteration; j++) {
        client.send(server.address(), Message.fromQualifier("q" + j));
      }
      latch.await(20, TimeUnit.SECONDS);
      long iterationTime = System.currentTimeMillis() - startAt;
      if (i > 0) { // exclude warm up iteration
        iterationTimeSeries[i - 1] = iterationTime;
      }
      assertSendOrder(sentPerIteration, received);

      LOGGER.info("Iteration time: {} ms", iterationTime);

      serverSubscriber.unsubscribe();
      destroyTransport(client);
    }

    LongSummaryStatistics iterationTimeStats = LongStream.of(iterationTimeSeries).summaryStatistics();
    LOGGER.info("Iteration time stats (ms): {}", iterationTimeStats);
  }

  @Test
  public void testSendOrderSingleThread() throws Exception {
    server = createTransport();

    int iterationNum = 11; // +1 warm up iteration
    int sentPerIteration = 1000;
    long[] iterationTimeSeries = new long[iterationNum - 1];
    List<Long> totalSentTimeSeries = new ArrayList<>(sentPerIteration * (iterationNum - 1));
    for (int i = 0; i < iterationNum; i++) {
      LOGGER.info("####### {} : iteration = {}", testName.getMethodName(), i);
      List<Long> iterSentTimeSeries = new ArrayList<>(sentPerIteration);

      client = createTransport();
      final List<Message> received = new ArrayList<>();
      final CountDownLatch latch = new CountDownLatch(sentPerIteration);

      Subscriber<Message> serverSubscriber = Subscribers.create(message -> {
        received.add(message);
        latch.countDown();
      });
      server.listen().subscribe(serverSubscriber);

      long startAt = System.currentTimeMillis();
      for (int j = 0; j < sentPerIteration; j++) {
        CompletableFuture<Void> sendPromise = new CompletableFuture<>();
        long sentAt = System.currentTimeMillis();
        client.send(server.address(), Message.fromQualifier("q" + j), sendPromise);
        sendPromise.whenComplete((aVoid, exception) -> {
          if (exception == null) {
            long sentTime = System.currentTimeMillis() - sentAt;
            iterSentTimeSeries.add(sentTime);
          } else {
            LOGGER.error("Failed to send message in {} ms", System.currentTimeMillis() - sentAt, exception);
          }
        });
      }

      latch.await(20, TimeUnit.SECONDS);
      long iterationTime = System.currentTimeMillis() - startAt;
      if (i > 0) { // exclude warm up iteration
        iterationTimeSeries[i - 1] = iterationTime;
      }
      assertSendOrder(sentPerIteration, received);

      Thread.sleep(10); // await a bit for last msg confirmation

      LongSummaryStatistics iterSentTimeStats = iterSentTimeSeries.stream().mapToLong(v -> v).summaryStatistics();
      if (i == 0) { // warm up iteration
        LOGGER.info("Warm up iteration time: {} ms", iterationTime);
        LOGGER.info("Sent time stats warm up iter (ms): {}", iterSentTimeStats);
      } else {
        totalSentTimeSeries.addAll(iterSentTimeSeries);
        LongSummaryStatistics totalSentTimeStats = totalSentTimeSeries.stream().mapToLong(v -> v).summaryStatistics();
        LOGGER.info("Iteration time: {} ms", iterationTime);
        LOGGER.info("Sent time stats iter  (ms): {}", iterSentTimeStats);
        LOGGER.info("Sent time stats total (ms): {}", totalSentTimeStats);
      }

      serverSubscriber.unsubscribe();
      destroyTransport(client);
    }

    LongSummaryStatistics iterationTimeStats = LongStream.of(iterationTimeSeries).summaryStatistics();
    LOGGER.info("Iteration time stats (ms): {}", iterationTimeStats);
  }

  @Test
  public void testSendOrderMultiThread() throws Exception {
    Transport server = createTransport();

    final int total = 1000;
    for (int i = 0; i < 10; i++) {
      LOGGER.info("####### {} : iteration = {}", testName.getMethodName(), i);
      ExecutorService exec = Executors.newFixedThreadPool(4, new ThreadFactoryBuilder().setDaemon(true).build());

      Transport client = createTransport();
      final List<Message> received = new ArrayList<>();
      final CountDownLatch latch = new CountDownLatch(4 * total);
      server.listen().subscribe(message -> {
        received.add(message);
        latch.countDown();
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

      exec.shutdownNow();

      assertSenderOrder(0, total, received);
      assertSenderOrder(1, total, received);
      assertSenderOrder(2, total, received);
      assertSenderOrder(3, total, received);

      destroyTransport(client);
    }

    destroyTransport(client);
    destroyTransport(server);
  }

  private void assertSendOrder(int total, List<Message> received) {
    ArrayList<Message> messages = new ArrayList<>(received);
    assertEquals(total, messages.size());
    for (int k = 0; k < total; k++) {
      assertEquals("q" + k, messages.get(k).qualifier());
    }
  }

  private Callable<Void> sender(final int id, final Transport client, final Address address, final int total) {
    return () -> {
      for (int j = 0; j < total; j++) {
        String correlationId = id + "/" + j;
        CompletableFuture<Void> sendPromise = new CompletableFuture<>();
        client.send(address, Message.withQualifier("q").correlationId(correlationId).build(), sendPromise);
        try {
          sendPromise.get(3, TimeUnit.SECONDS);
        } catch (Exception e) {
          LOGGER.error("Failed to send message: j = {} id = {}", j, id, e);
          propagate(e);
        }
      }
      return null;
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

}
