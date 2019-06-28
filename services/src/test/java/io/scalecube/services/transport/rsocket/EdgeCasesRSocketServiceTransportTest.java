package io.scalecube.services.transport.rsocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.services.BaseTest;
import io.scalecube.services.Microservices;
import io.scalecube.services.annotations.Inject;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Disabled
class EdgeCasesRSocketServiceTransportTest extends BaseTest {

  private Microservices gateway;
  private Microservices service1Node;
  private Microservices service2Node;

  @BeforeEach
  void setUp() {
    gateway =
        Microservices.builder()
            .discovery(ScalecubeServiceDiscovery::new)
            .transport(opts -> opts.serviceTransport(RSocketServiceTransport::new))
            .startAwait();

    service2Node =
        Microservices.builder()
            .discovery(
                serviceEndpoint ->
                    new ScalecubeServiceDiscovery(serviceEndpoint)
                        .options(opts -> opts.seedMembers(gateway.discovery().address())))
            .transport(opts -> opts.serviceTransport(RSocketServiceTransport::new))
            .services(new Service2Impl())
            .startAwait();

    service1Node =
        Microservices.builder()
            .discovery(
                serviceEndpoint ->
                    new ScalecubeServiceDiscovery(serviceEndpoint)
                        .options(opts -> opts.seedMembers(gateway.discovery().address())))
            .transport(opts -> opts.serviceTransport(RSocketServiceTransport::new))
            .services(new Service1Impl())
            .startAwait();
  }

  @AfterEach
  void cleanUp() {
    try {
      Mono.whenDelayError(
              Optional.ofNullable(gateway).map(Microservices::shutdown).orElse(Mono.empty()),
              Optional.ofNullable(service1Node).map(Microservices::shutdown).orElse(Mono.empty()),
              Optional.ofNullable(service2Node).map(Microservices::shutdown).orElse(Mono.empty()))
          .block();
    } catch (Throwable ignore) {
      // no-op
    }
  }

  @Test
  void testMany() {
    gateway
        .call()
        .api(Service1.class)
        .manyDelay(100)
        .publishOn(Schedulers.parallel())
        .take(10)
        .log("receive     |")
        .collectList()
        .log("complete    |")
        .block();
  }

  @Test
  void testRemoteCallThenMany() {
    gateway
        .call()
        .api(Service1.class)
        .remoteCallThenManyDelay(100)
        .publishOn(Schedulers.parallel())
        .take(10)
        .log("receive     |")
        .collectList()
        .log("complete    |")
        .block();
  }

  @Test
  void testMultiSend() throws InterruptedException {

    int nThreads = Runtime.getRuntime().availableProcessors();
    CountDownLatch latch = new CountDownLatch(nThreads * 2);

    ExecutorService executorService = Executors.newFixedThreadPool(nThreads);

    for (int i = 0; i < nThreads * 2; i++) {
      executorService.execute(
          () -> {
            gateway
                .call()
                .api(Service1.class)
                .remoteCallThenManyDelay(10000)
                .publishOn(Schedulers.parallel())
                .take(10)
                .log("receive     |")
                .collectList()
                .log("complete    |")
                .block();

            latch.countDown();
          });
    }

    assertTrue(latch.await(200, TimeUnit.SECONDS));
    assertEquals(0, latch.getCount());
  }

  @Service
  public interface Service1 {

    @ServiceMethod
    Flux<String> manyDelay(long interval);

    @ServiceMethod
    Flux<String> remoteCallThenManyDelay(long interval);
  }

  @Service
  public interface Service2 {

    @ServiceMethod
    Mono<String> oneDelay(long interval);
  }

  private static class Service1Impl implements Service1 {

    @Inject private Service2 remoteService;

    @Override
    public Flux<String> manyDelay(long interval) {
      return Flux.<String>create(
              sink ->
                  sink.onRequest(
                      r -> {
                        long lastPublished = System.currentTimeMillis();

                        while (!sink.isCancelled() && sink.requestedFromDownstream() > 0) {
                          long now = System.currentTimeMillis();

                          if (sink.requestedFromDownstream() > 0) {
                            if (now - lastPublished > interval) {
                              lastPublished = now;
                              sink.next(toResponse(now));
                              continue;
                            }
                          }

                          LockSupport.parkNanos(SLEEP_PERIOD_NS);
                        }
                      }))
          .subscribeOn(Schedulers.parallel())
          .log("manyDelay   |");
    }

    @Override
    public Flux<String> remoteCallThenManyDelay(long interval) {
      return remoteService
          .oneDelay(interval)
          .log("remoteCall  |")
          .publishOn(Schedulers.parallel())
          .flatMapMany(
              i ->
                  Flux.<String>create(
                          sink ->
                              sink.onRequest(
                                  r -> {
                                    long lastPublished = System.currentTimeMillis();

                                    while (!sink.isCancelled()
                                        && sink.requestedFromDownstream() > 0) {
                                      long now = System.currentTimeMillis();

                                      if (sink.requestedFromDownstream() > 0) {
                                        if (now - lastPublished > interval) {
                                          lastPublished = now;
                                          sink.next(toResponse(now));
                                          continue;
                                        }
                                      }

                                      LockSupport.parkNanos(SLEEP_PERIOD_NS);
                                    }
                                  }))
                      .log("manyInner   |"))
          .log("rcManyDelay |");
    }
  }

  private static class Service2Impl implements Service2 {

    @Override
    public Mono<String> oneDelay(long interval) {

      return Mono.<String>create(
              sink -> {
                AtomicBoolean isActive = new AtomicBoolean(true);
                sink.onCancel(() -> isActive.set(false));
                sink.onDispose(() -> isActive.set(false));

                long started = System.currentTimeMillis();

                sink.onRequest(
                    r -> {
                      while (isActive.get()) {
                        long now = System.currentTimeMillis();

                        if (now - started > interval) {
                          sink.success(toResponse(now));
                          return;
                        }

                        LockSupport.parkNanos(SLEEP_PERIOD_NS);
                      }
                    });
              })
          .subscribeOn(Schedulers.parallel())
          .log("oneDelay    |");
    }
  }

  private static final long SLEEP_PERIOD_NS = 10000;

  private static String toResponse(long now) {
    String currentThread = Thread.currentThread().getName();
    final LocalDateTime time =
        LocalDateTime.ofInstant(Instant.ofEpochMilli(now), ZoneId.systemDefault());
    return "|" + currentThread + "| response: " + time;
  }
}
