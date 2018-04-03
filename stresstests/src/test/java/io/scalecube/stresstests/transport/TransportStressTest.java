package io.scalecube.stresstests.transport;

import io.scalecube.transport.Message;
import io.scalecube.transport.Transport;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.scalecube.testlib.BaseTest;

import static io.scalecube.transport.TransportTestUtils.destroyTransport;
import static org.junit.Assert.assertTrue;

/**
 * @author Anton Kharenko
 */
@RunWith(Parameterized.class)
public class TransportStressTest extends BaseTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransportStressTest.class);

  private static List<Object[]> experiments = Arrays.asList(new Object[][] {
      // Msg count
      {     1_000 }, // warm up
      {     1_000 },
      {     5_000 },
      {    10_000 },
      {    25_000 },
//      {    50_000 },
//      {   100_000 },
//      {   250_000 },
//      {   500_000 },
//      { 1_000_000 },
  });

  // Maximum time to await for all responses
  private static final int timeoutSeconds = 60;

  @Parameterized.Parameters(name = "msgCount={0}")
  public static List<Object[]> data() {
    return experiments;
  }

  private final int msgCount;

  public TransportStressTest(int msgCount) {
    this.msgCount = msgCount;
  }

  @Test
  public void transportStressTest() throws Exception {
    // Init transports
    Transport echoServer = Transport.bindAwait();
    Transport client1 = null;

    // Init measured params
    long sentTime = 0;
    long receivedTime = 0;
    LongSummaryStatistics rttStats = null;

    // Run experiment
    try {
      // Subscribe echo server handler
      echoServer.listen().subscribe(msg -> echoServer.send(msg.sender(), msg));

      // Init client
      CountDownLatch measureLatch = new CountDownLatch(msgCount);
      ArrayList<Long> rttRecords = new ArrayList<>(msgCount);
      client1 = Transport.bindAwait();
      client1.listen().subscribe(msg -> {
        long sentAt = Long.valueOf(msg.data());
        long rttTime = System.currentTimeMillis() - sentAt;
        rttRecords.add(rttTime);
        measureLatch.countDown();
      });

      // Measure
      long startAt = System.currentTimeMillis();
      for (int i = 0; i < msgCount; i++) {
        client1.send(echoServer.address(), Message.fromData(Long.toString(System.currentTimeMillis())));
      }
      sentTime = System.currentTimeMillis() - startAt;
      measureLatch.await(timeoutSeconds, TimeUnit.SECONDS);
      receivedTime = System.currentTimeMillis() - startAt;
      rttStats = rttRecords.stream().mapToLong(v -> v).summaryStatistics();
      assertTrue(measureLatch.getCount() == 0);
    } finally {
      // Print results
      LOGGER.info("Finished sending {} messages in {} ms", msgCount, sentTime);
      LOGGER.info("Finished receiving {} messages in {} ms", msgCount, receivedTime);
      LOGGER.info("Round trip stats (ms): {}", rttStats);

      // Destroy transport
      destroyTransport(echoServer);
      destroyTransport(client1);
    }
  }

}
