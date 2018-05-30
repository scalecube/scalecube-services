package io.scalecube.services.benchmarks;

import static io.scalecube.services.CommunicationMode.REQUEST_RESPONSE;

import io.scalecube.cluster.membership.IdGenerator;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceMethodDefinition;
import io.scalecube.services.ServiceRegistration;
import io.scalecube.services.api.Qualifier;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.registry.ServiceRegistryImpl;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.routing.RandomServiceRouter;
import io.scalecube.services.routing.RoundRobinServiceRouter;
import io.scalecube.services.routing.Router;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Fork(1)
@Threads(Threads.MAX)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
public class ServiceRouterBenchmarks {
  private static final String NAMESPACE = "benchmark";
  private static final String ACTION = "test";
  private static final String CONTENT_TYPE = "application/json";
  private static final ServiceMessage MESSAGE = ServiceMessage.builder()
      .qualifier(Qualifier.asString(NAMESPACE, ACTION))
      .data("{\"greeting\":\"hello\"}")
      .dataFormat(CONTENT_TYPE)
      .build();

  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @Benchmark
  public void roundRobinServiceRouterOneRegisteredAverageTime(RoundRobinServiceRouterState routerState,
      ServiceRegistryWithOneRegisteredState serviceRegistryState,
      Blackhole bh) {
    route(routerState.router, serviceRegistryState.serviceRegistry, bh);
  }

  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Benchmark
  public void roundRobinServiceRouterOneRegisteredThroughput(RoundRobinServiceRouterState routerState,
      ServiceRegistryWithOneRegisteredState serviceRegistryState,
      Blackhole bh) {
    route(routerState.router, serviceRegistryState.serviceRegistry, bh);
  }

  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @Benchmark
  public void roundRobinServiceRouterTenRegisteredAverageTime(RoundRobinServiceRouterState routerState,
      ServiceRegistryWithTenRegisteredState serviceRegistryState,
      Blackhole bh) {
    route(routerState.router, serviceRegistryState.serviceRegistry, bh);
  }

  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Benchmark
  public void roundRobinServiceRouterTenRegisteredThroughput(RoundRobinServiceRouterState routerState,
      ServiceRegistryWithTenRegisteredState serviceRegistryState,
      Blackhole bh) {
    route(routerState.router, serviceRegistryState.serviceRegistry, bh);
  }

  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @Benchmark
  public void randomServiceRouterOneRegisteredAverageTime(RandomServiceRouterState routerState,
      ServiceRegistryWithOneRegisteredState serviceRegistryState,
      Blackhole bh) {
    route(routerState.router, serviceRegistryState.serviceRegistry, bh);
  }

  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Benchmark
  public void randomServiceRouterOneRegisteredThroughput(RandomServiceRouterState routerState,
      ServiceRegistryWithOneRegisteredState serviceRegistryState,
      Blackhole bh) {
    route(routerState.router, serviceRegistryState.serviceRegistry, bh);
  }

  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @Benchmark
  public void randomServiceRouterTenRegisteredAverageTime(RandomServiceRouterState routerState,
      ServiceRegistryWithTenRegisteredState serviceRegistryState,
      Blackhole bh) {
    route(routerState.router, serviceRegistryState.serviceRegistry, bh);
  }

  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Benchmark
  public void randomServiceRouterTenRegisteredThroughput(RandomServiceRouterState routerState,
      ServiceRegistryWithTenRegisteredState serviceRegistryState,
      Blackhole bh) {
    route(routerState.router, serviceRegistryState.serviceRegistry, bh);
  }

  private void route(Router router, ServiceRegistry serviceRegistry, Blackhole bh) {
    bh.consume(router.route(serviceRegistry, MESSAGE));
  }

  @State(Scope.Benchmark)
  public static class RoundRobinServiceRouterState {
    private Router router = new RoundRobinServiceRouter();
  }

  @State(Scope.Benchmark)
  public static class RandomServiceRouterState {
    private Router router = new RandomServiceRouter();
  }

  @State(Scope.Benchmark)
  public static class ServiceRegistryWithOneRegisteredState {
    private ServiceRegistryImpl serviceRegistry = new ServiceRegistryImpl();
    {
      registrer(serviceRegistry, 1);
    }
  }

  @State(Scope.Benchmark)
  public static class ServiceRegistryWithTenRegisteredState {
    private ServiceRegistryImpl serviceRegistry = new ServiceRegistryImpl();
    {
      registrer(serviceRegistry, 10);
    }
  }

  private static void registrer(ServiceRegistryImpl serviceRegistry, int count) {
    Map<String, String> tags = Collections.emptyMap();
    Collection<ServiceMethodDefinition> methods = Collections.singletonList(
        new ServiceMethodDefinition(ACTION, CONTENT_TYPE, tags, REQUEST_RESPONSE));
    Collection<ServiceRegistration> serviceRegistrations = Collections.singletonList(
        new ServiceRegistration(NAMESPACE, CONTENT_TYPE, tags, methods));
    for (int i = 0; i < count; i++) {
      serviceRegistry.registerService(
          new ServiceEndpoint(IdGenerator.generateId(), "localhost", 8080 + i, tags, serviceRegistrations));
    }
  }
}
