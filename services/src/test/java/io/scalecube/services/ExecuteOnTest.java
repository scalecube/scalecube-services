package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.services.Microservices.Context;
import io.scalecube.services.annotations.ExecuteOn;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class ExecuteOnTest extends BaseTest {

  private static final String SCHEDULER1_NAME = "scheduler@1";
  private static final String SCHEDULER2_NAME = "scheduler@2";
  private static final String SCHEDULER3_NAME = "scheduler@3@that-was-not-declared";

  private final Map<String, Scheduler> schedulers = new ConcurrentHashMap<>();

  @BeforeEach
  void beforeEach() {
    schedulers.computeIfAbsent(SCHEDULER1_NAME, ExecuteOnTest::scheduler);
    schedulers.computeIfAbsent(SCHEDULER2_NAME, ExecuteOnTest::scheduler);
  }

  private static Scheduler scheduler(String s) {
    return Schedulers.fromExecutor(
        Executors.newSingleThreadExecutor(
            r -> {
              final var thread = new Thread(r);
              thread.setDaemon(true);
              thread.setName(s);
              return thread;
            }));
  }

  @AfterEach
  void afterEach() {
    schedulers.forEach((s, scheduler) -> scheduler.dispose());
    schedulers.clear();
  }

  @Test
  void testExecuteOnClass() {
    final var executeOnClass = new HelloServiceV1();
    try (final var microservices =
        Microservices.start(
            new Context()
                .scheduler(SCHEDULER1_NAME, () -> schedulers.get(SCHEDULER1_NAME))
                .scheduler(SCHEDULER2_NAME, () -> schedulers.get(SCHEDULER2_NAME))
                .services(executeOnClass))) {

      final var api = microservices.call().api(HelloService.class);

      api.hello().block();
      assertEquals(SCHEDULER1_NAME, executeOnClass.threadName.get(), "threadName");

      api.hola().block();
      assertEquals(SCHEDULER1_NAME, executeOnClass.threadName.get(), "threadName");

      api.arigato().block();
      assertEquals(SCHEDULER1_NAME, executeOnClass.threadName.get(), "threadName");
    }
  }

  @Test
  void testExecuteOnMethod() {
    final var executeOnClass = new HelloServiceV2();
    try (final var microservices =
        Microservices.start(
            new Context()
                .scheduler(SCHEDULER1_NAME, () -> schedulers.get(SCHEDULER1_NAME))
                .scheduler(SCHEDULER2_NAME, () -> schedulers.get(SCHEDULER2_NAME))
                .services(executeOnClass))) {

      final var api = microservices.call().api(HelloService.class);

      api.hello().block();
      assertEquals(SCHEDULER1_NAME, executeOnClass.threadName.get(), "threadName");

      api.hola().block();
      assertEquals(SCHEDULER2_NAME, executeOnClass.threadName.get(), "threadName");

      api.arigato().block();
      assertEquals("main", executeOnClass.threadName.get(), "threadName");
    }
  }

  @Test
  void testExecuteOnMixedDefinition() {
    final var executeOnClass = new HelloServiceV3();
    try (final var microservices =
        Microservices.start(
            new Context()
                .scheduler(SCHEDULER1_NAME, () -> schedulers.get(SCHEDULER1_NAME))
                .scheduler(SCHEDULER2_NAME, () -> schedulers.get(SCHEDULER2_NAME))
                .services(executeOnClass))) {

      final var api = microservices.call().api(HelloService.class);

      api.hello().block();
      assertEquals(SCHEDULER1_NAME, executeOnClass.threadName.get(), "threadName");

      api.hola().block();
      assertEquals(SCHEDULER1_NAME, executeOnClass.threadName.get(), "threadName");

      api.arigato().block();
      assertEquals(SCHEDULER2_NAME, executeOnClass.threadName.get(), "threadName");
    }
  }

  @Test
  void testExecuteOnSchedulerThatWasNotDeclared() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          //noinspection unused,EmptyTryBlock
          try (final var microservices =
              Microservices.start(
                  new Context()
                      .scheduler(SCHEDULER1_NAME, () -> schedulers.get(SCHEDULER1_NAME))
                      .scheduler(SCHEDULER2_NAME, () -> schedulers.get(SCHEDULER2_NAME))
                      .services(new HelloServiceV4()))) {}
        });
  }

  @Test
  void testExecuteOnSchedulerMustBeDisposed() {
    final var s1 = Schedulers.newSingle("s1");
    final var s2 = Schedulers.newSingle("s2");
    final var s3 = Schedulers.newSingle("s3");

    //noinspection unused,EmptyTryBlock
    try (final var microservices =
        Microservices.start(
            new Context()
                .scheduler("s1", () -> s1)
                .scheduler("s2", () -> s2)
                .scheduler("s3", () -> s3))) {}

    assertTrue(s1.isDisposed(), "s1.isDisposed");
    assertTrue(s2.isDisposed(), "s2.isDisposed");
    assertTrue(s3.isDisposed(), "s3.isDisposed");
  }

  @Service("v1/greeting")
  public interface HelloService {

    @ServiceMethod
    Mono<String> hello();

    @ServiceMethod
    Mono<String> hola();

    @ServiceMethod
    Mono<String> arigato();
  }

  // All methods must be executed in scheduler@1
  @ExecuteOn(SCHEDULER1_NAME)
  public static class HelloServiceV1 implements HelloService {

    final AtomicReference<String> threadName = new AtomicReference<>();

    @Override
    public Mono<String> hello() {
      threadName.set(Thread.currentThread().getName());
      return Mono.just("Hello | " + System.currentTimeMillis());
    }

    @Override
    public Mono<String> hola() {
      threadName.set(Thread.currentThread().getName());
      return Mono.just("Hola | " + System.currentTimeMillis());
    }

    @Override
    public Mono<String> arigato() {
      threadName.set(Thread.currentThread().getName());
      return Mono.just("Arigato | " + System.currentTimeMillis());
    }
  }

  public static class HelloServiceV2 implements HelloService {

    final AtomicReference<String> threadName = new AtomicReference<>();

    // This method must be executed in the scheduler@1
    @ExecuteOn(SCHEDULER1_NAME)
    @Override
    public Mono<String> hello() {
      threadName.set(Thread.currentThread().getName());
      return Mono.just("Hello | " + System.currentTimeMillis());
    }

    // This method must be executed in the scheduler@2
    @ExecuteOn(SCHEDULER2_NAME)
    @Override
    public Mono<String> hola() {
      threadName.set(Thread.currentThread().getName());
      return Mono.just("Hola | " + System.currentTimeMillis());
    }

    // This method must be executed in the caller thread
    @Override
    public Mono<String> arigato() {
      threadName.set(Thread.currentThread().getName());
      return Mono.just("Arigato | " + System.currentTimeMillis());
    }
  }

  // All methods must be executed in scheduler@1 unless they override on service method
  @ExecuteOn(SCHEDULER1_NAME)
  public static class HelloServiceV3 implements HelloService {

    final AtomicReference<String> threadName = new AtomicReference<>();

    @Override
    public Mono<String> hello() {
      threadName.set(Thread.currentThread().getName());
      return Mono.just("Hello | " + System.currentTimeMillis());
    }

    @Override
    public Mono<String> hola() {
      threadName.set(Thread.currentThread().getName());
      return Mono.just("Hola | " + System.currentTimeMillis());
    }

    // This method must be executed in the scheduler@2
    @ExecuteOn(SCHEDULER2_NAME)
    @Override
    public Mono<String> arigato() {
      threadName.set(Thread.currentThread().getName());
      return Mono.just("Arigato | " + System.currentTimeMillis());
    }
  }

  // All methods must be executed in scheduler@3@that-was-not-declared
  @ExecuteOn(SCHEDULER3_NAME)
  public static class HelloServiceV4 implements HelloService {

    final AtomicReference<String> threadName = new AtomicReference<>();

    @Override
    public Mono<String> hello() {
      threadName.set(Thread.currentThread().getName());
      return Mono.just("Hello | " + System.currentTimeMillis());
    }

    @Override
    public Mono<String> hola() {
      threadName.set(Thread.currentThread().getName());
      return Mono.just("Hola | " + System.currentTimeMillis());
    }

    @Override
    public Mono<String> arigato() {
      threadName.set(Thread.currentThread().getName());
      return Mono.just("Arigato | " + System.currentTimeMillis());
    }
  }
}
