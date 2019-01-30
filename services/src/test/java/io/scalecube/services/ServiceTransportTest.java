package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import io.scalecube.services.exceptions.ConnectionClosedException;
import io.scalecube.services.sut.QuoteService;
import io.scalecube.services.sut.SimpleQuoteService;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.HeadersCodec;
import io.scalecube.services.transport.api.ServerTransport;
import io.scalecube.services.transport.api.ServiceMessageCodec;
import io.scalecube.services.transport.api.ServiceTransport;
import io.scalecube.services.transport.rsocket.aeron.RSocketAeronClientTransport;
import io.scalecube.services.transport.rsocket.aeron.RSocketAeronServerTransport;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.aeron.AeronResources;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

public class ServiceTransportTest {

  private static final ServiceMessage JUST_NEVER =
      ServiceMessage.builder().qualifier(QuoteService.NAME, "justNever").build();
  private static final ServiceMessage JUST_MANY_NEVER =
      ServiceMessage.builder().qualifier(QuoteService.NAME, "justManyNever").build();
  private static final ServiceMessage ONLY_ONE_AND_THEN_NEVER =
      ServiceMessage.builder().qualifier(QuoteService.NAME, "onlyOneAndThenNever").build();

  private Microservices gateway;
  private Microservices serviceNode;

  /** Setup. */
  @BeforeEach
  public void setUp() {
    gateway =
        Microservices.builder()
            .transport(options -> options.transport(new RSocketAeronTestServiceTransport()))
            .startAwait();

    serviceNode =
        Microservices.builder()
            .transport(options -> options.transport(new RSocketAeronTestServiceTransport()))
            .discovery(options -> options.seeds(gateway.discovery().address()))
            .services(new SimpleQuoteService())
            .startAwait();
  }

  /** Cleanup. */
  @AfterEach
  public void cleanUp() {
    if (gateway != null) {
      try {
        gateway.shutdown();
      } catch (Throwable ignore) {
        // no-op
      }
    }
    if (serviceNode != null) {
      try {
        serviceNode.shutdown();
      } catch (Throwable ignore) {
        // no-op
      }
    }
  }

  @Test
  public void test_remote_node_died_mono_never() throws Exception {
    int batchSize = 1;

    final CountDownLatch latch1 = new CountDownLatch(batchSize);
    AtomicReference<Disposable> sub1 = new AtomicReference<>(null);
    AtomicReference<Throwable> exceptionHolder = new AtomicReference<>(null);

    ServiceCall serviceCall = gateway.call().create();
    sub1.set(serviceCall.requestOne(JUST_NEVER).doOnError(exceptionHolder::set).subscribe());

    gateway
        .discovery()
        .listen()
        .filter(ServiceDiscoveryEvent::isUnregistered)
        .subscribe(onNext -> latch1.countDown(), System.err::println);

    // service node goes down
    TimeUnit.SECONDS.sleep(3);
    serviceNode.shutdown().block(Duration.ofSeconds(6));

    latch1.await(20, TimeUnit.SECONDS);
    TimeUnit.MILLISECONDS.sleep(100);

    assertEquals(0, latch1.getCount());
    assertEquals(ConnectionClosedException.class, exceptionHolder.get().getClass());
    assertTrue(sub1.get().isDisposed());
  }

  @Test
  public void test_remote_node_died_many_never() throws Exception {
    int batchSize = 1;

    final CountDownLatch latch1 = new CountDownLatch(batchSize);
    AtomicReference<Disposable> sub1 = new AtomicReference<>(null);
    AtomicReference<Throwable> exceptionHolder = new AtomicReference<>(null);

    ServiceCall serviceCall = gateway.call().create();
    sub1.set(serviceCall.requestMany(JUST_MANY_NEVER).doOnError(exceptionHolder::set).subscribe());

    gateway
        .discovery()
        .listen()
        .filter(ServiceDiscoveryEvent::isUnregistered)
        .subscribe(onNext -> latch1.countDown(), System.err::println);

    // service node goes down
    TimeUnit.SECONDS.sleep(3);
    serviceNode.shutdown().block(Duration.ofSeconds(6));

    latch1.await(20, TimeUnit.SECONDS);
    TimeUnit.MILLISECONDS.sleep(1000);

    assertEquals(0, latch1.getCount());
    assertEquals(ConnectionClosedException.class, exceptionHolder.get().getClass());
    assertTrue(sub1.get().isDisposed());
  }

  @Test
  public void test_remote_node_died_many_then_never() throws Exception {
    int batchSize = 1;

    final CountDownLatch latch1 = new CountDownLatch(batchSize);
    AtomicReference<Disposable> sub1 = new AtomicReference<>(null);
    AtomicReference<Throwable> exceptionHolder = new AtomicReference<>(null);

    ServiceCall serviceCall = gateway.call().create();
    sub1.set(
        serviceCall
            .requestMany(ONLY_ONE_AND_THEN_NEVER)
            .doOnError(exceptionHolder::set)
            .subscribe());

    gateway
        .discovery()
        .listen()
        .filter(ServiceDiscoveryEvent::isUnregistered)
        .subscribe(onNext -> latch1.countDown(), System.err::println);

    // service node goes down
    TimeUnit.SECONDS.sleep(3);
    serviceNode.shutdown().block(Duration.ofSeconds(6));

    latch1.await(20, TimeUnit.SECONDS);
    TimeUnit.MILLISECONDS.sleep(100);

    assertEquals(0, latch1.getCount());

    assertEquals(ConnectionClosedException.class, exceptionHolder.get().getClass());
    assertTrue(sub1.get().isDisposed());
  }

  public static class RSocketAeronTestServiceTransport implements ServiceTransport {

    private static final String DEFAULT_HEADERS_FORMAT = "application/json";

    @Override
    public ServiceTransport.Resources resources(int numOfWorkers) {
      return new Resources(numOfWorkers);
    }

    @Override
    public ClientTransport clientTransport(ServiceTransport.Resources resources) {
      return new RSocketAeronClientTransport(
          new ServiceMessageCodec(HeadersCodec.getInstance(DEFAULT_HEADERS_FORMAT)),
          ((Resources) resources).aeronResources);
    }

    @Override
    public ServerTransport serverTransport(ServiceTransport.Resources resources) {
      return new RSocketAeronServerTransport(
          new ServiceMessageCodec(HeadersCodec.getInstance(DEFAULT_HEADERS_FORMAT)),
          ((Resources) resources).aeronResources);
    }

    private static class Resources implements ServiceTransport.Resources {

      private final AeronResources aeronResources;

      private Resources(int numOfWorkers) {
        aeronResources =
            new AeronResources()
                .useTmpDir()
                .numOfWorkers(numOfWorkers)
                .media(ctx -> ctx.imageLivenessTimeoutNs(Duration.ofSeconds(1).toNanos()))
                .start()
                .block();
      }

      @Override
      public Optional<Executor> workerPool() {
        // worker pool is not exposed in aeron
        return Optional.empty();
      }

      @Override
      public Mono<Void> shutdown() {
        return Mono.defer(
            () -> {
              aeronResources.dispose();
              return aeronResources.onDispose();
            });
      }
    }
  }
}
