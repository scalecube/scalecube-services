package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.UnicastProcessor;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

public class HeadAndTailTest {

  @Test
  public void testWithUnicastFromStream() {
    Integer first = 1;
    int size = 10;
    List<Integer> source = IntStream.rangeClosed(first, size).boxed().collect(Collectors.toList());
    Flux<Integer> requests = UnicastProcessor.fromStream(source.stream());

    Integer[] actual =
        Flux.from(HeadAndTail.createFrom(requests))
            .flatMap(
                pair -> {
                  assertEquals(first, pair.head());
                  return Flux.from(pair.tail());
                })
            .toStream()
            .toArray(Integer[]::new);
    assertArrayEquals(source.stream().skip(1).toArray(), actual);
  }

  @Test
  public void testWithUnicastFromInterval() throws InterruptedException {
    Long first = 1L;
    int size = 10;
    AtomicLong counter = new AtomicLong(first);
    CountDownLatch latch = new CountDownLatch(1);
    Long[] expectedTail =
        LongStream.rangeClosed(first, size + first - 1).boxed().skip(1).toArray(Long[]::new);
    Flux<Long> requests =
        UnicastProcessor.from(
            Flux.interval(Duration.ofMillis(50), Schedulers.parallel())
                .map(ignore -> counter.getAndIncrement())
                .doOnCancel(latch::countDown)
                .take(size));

    Long[] actual =
        Flux.from(HeadAndTail.createFrom(requests))
            .flatMap(
                pair -> {
                  assertEquals(first, pair.head());
                  return Flux.from(pair.tail());
                })
            .toStream()
            .toArray(Long[]::new);
    assertArrayEquals(expectedTail, actual);

    latch.await();
  }

  @Test
  public void testWithUnicastWithTailException() {
    Long first = 1L;
    Flux<Long> requests =
        UnicastProcessor.create(
            emitter -> {
              emitter.next(1L);
              emitter.next(2L);
              emitter.next(3L);
              emitter.error(new RuntimeException());
            });

    StepVerifier.create(
            Flux.from(HeadAndTail.createFrom(requests))
                .flatMap(
                    pair -> {
                      assertEquals(first, pair.head());
                      return Flux.from(pair.tail());
                    }))
        .expectNext(2L)
        .expectNext(3L)
        .expectError(RuntimeException.class);
  }

  @Test
  public void testWithUnicastWithHeadException() {
    Flux<Long> requests = UnicastProcessor.create(emitter -> emitter.error(new RuntimeException()));

    StepVerifier.create(
            Flux.from(HeadAndTail.createFrom(requests))
                .flatMap(
                    pair -> {
                      fail("never");
                      return null;
                    }))
        .expectError(RuntimeException.class);
  }
}
