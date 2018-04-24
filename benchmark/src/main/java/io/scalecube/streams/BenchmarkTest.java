package io.scalecube.streams;

import io.scalecube.transport.Address;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import rx.Emitter;
import rx.Observable;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

@Fork(8)
@State(Scope.Benchmark)
@Threads(4)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class BenchmarkTest {

  private List<Emitter<Event>> emitters = new CopyOnWriteArrayList<>();

  private StreamMessage message;
  private Observable<Event> observable;
  private Event event;

  /**
   * setup the test.
   */
  @Setup
  public void setUp() {
    message = StreamMessage.builder().qualifier("q/yadayada").build();

    event = Event.readSuccess(Address.from("cc2:0")).message(message).identity("21dsf53gf").build();

    observable = Observable.create(emitter -> {
      emitter.setCancellation(() -> emitters.removeIf(emitter1 -> emitter1 == emitter));
      emitters.add(emitter);
    }, Emitter.BackpressureMode.BUFFER);

    observable.subscribe(event1 -> {
    });
  }

  @Benchmark
  public void testRawObservableSpeed() {
    emitters.forEach(emitter -> emitter.onNext(event));
  }
}
