package io.scalecube.services.benchmarks.routers;

import io.scalecube.benchmarks.BenchmarksSettings;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.Qualifier;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.registry.ServiceRegistryImpl;
import io.scalecube.services.routing.Router;

import com.codahale.metrics.Timer;

import java.util.concurrent.TimeUnit;

public class RouterBenchmarksRunner {

  private static final String NAMESPACE = "benchmark";
  private static final String ACTION = "method1";
  private static final ServiceMessage MESSAGE = ServiceMessage.builder()
      .qualifier(Qualifier.asString(NAMESPACE, ACTION))
      .build();

  public static void main(String[] args) {
    BenchmarksSettings settings = BenchmarksSettings.from(args).durationUnit(TimeUnit.NANOSECONDS).build();
    new RouterBenchmarksState(settings).runForSync(state -> {

      Timer timer = state.timer("timer");
      Router router = state.getRouter();
      ServiceRegistryImpl serviceRegistry = state.getServiceRegistry();

      return i -> {
        Timer.Context timeContext = timer.time();
        ServiceReference serviceReference = router.route(serviceRegistry, MESSAGE).get();
        timeContext.stop();
        return serviceReference;
      };
    });
  }
}
