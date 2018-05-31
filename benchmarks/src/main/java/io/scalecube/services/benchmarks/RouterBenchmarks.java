package io.scalecube.services.benchmarks;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.api.Qualifier;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.discovery.ServiceScanner;
import io.scalecube.services.registry.ServiceRegistryImpl;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.routing.RoundRobinServiceRouter;
import io.scalecube.services.routing.Router;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Fork(1)
@Threads(Threads.MAX)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
public class RouterBenchmarks {
  private static final String NAMESPACE = "benchmark";
  private static final String ACTION = "method1";
  private static final ServiceMessage MESSAGE = ServiceMessage.builder()
      .qualifier(Qualifier.asString(NAMESPACE, ACTION))
      .build();

  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @Benchmark
  public void roundRobinRouterAverageTime(RoundRobinRouterState state, Blackhole bh) {
    route(state.router, state.serviceRegistry, bh);
  }

  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Benchmark
  public void roundRobinRouterThroughput(RoundRobinRouterState state, Blackhole bh) {
    route(state.router, state.serviceRegistry, bh);
  }

  private void route(Router router, ServiceRegistry serviceRegistry, Blackhole bh) {
    bh.consume(router.route(serviceRegistry, MESSAGE));
  }

  @State(Scope.Benchmark)
  public static class RoundRobinRouterState {
    private final ServiceRegistryImpl serviceRegistry = new ServiceRegistryImpl();
    private final Router router = new RoundRobinServiceRouter();

    @Param({"1", "10", "100"})
    public int count;

    @Setup
    public void setUp() {
      List<Microservices.ServiceInfo> services =
          Collections.singletonList(new Microservices.ServiceInfo(new RouterBenchmarksServiceImpl()));
      IntStream.rangeClosed(0, count).forEach(i -> {
        Map<String, String> tags = new HashMap<>();
        tags.put("k1-" + i, "v1-" + i);
        tags.put("k2-" + i, "v2-" + i);
        ServiceEndpoint serviceEndpoint = ServiceScanner.scan(services, "localhost" + i, i, tags);
        serviceRegistry.registerService(serviceEndpoint);
      });
    }
  }

  @Service("benchmark")
  public interface RouterBenchmarksService {

    @ServiceMethod
    Mono<String> method1(String request);

    @ServiceMethod
    Flux<String> method2(int request);

    @ServiceMethod
    Mono<String> method3(String request);

    @ServiceMethod
    Flux<String> method4(int request);
  }

  public static class RouterBenchmarksServiceImpl implements RouterBenchmarksService {

    @Override
    public Mono<String> method1(String request) {
      return null;
    }

    @Override
    public Flux<String> method2(int request) {
      return null;
    }

    @Override
    public Mono<String> method3(String request) {
      return null;
    }

    @Override
    public Flux<String> method4(int request) {
      return null;
    }
  }
}
