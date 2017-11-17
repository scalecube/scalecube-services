package io.scalecube.metrics.codahale;

import static org.junit.Assert.assertEquals;

import io.scalecube.metrics.api.Counter;
import io.scalecube.metrics.api.Gauge;
import io.scalecube.metrics.api.Meter;
import io.scalecube.metrics.api.MetricFactory;
import io.scalecube.testlib.BaseTest;

import com.codahale.metrics.MetricRegistry;

import org.junit.Test;

public class CodahaleMetricsFactoryTest extends BaseTest {

  private final MetricRegistry metrics = new MetricRegistry();

  private final MetricFactory factory = new CodahaleMetricsFactory(metrics);

  @Test
  public void testCounter() throws Exception {

    Counter counter = factory.counter().get("greetingService", "sayHello");

    counter.inc(1);
    assertEquals(counter.getCount(), 1);

    counter.dec();
    assertEquals(counter.getCount(), 0);

    counter.inc();
    assertEquals(counter.getCount(), 1);

    counter.dec();
    assertEquals(counter.getCount(), 0);

  }

  @Test
  public void testMeter() throws Exception {
    Meter meter = factory.meter().get("greetingService", "sayHello", "requests");
    meter.mark(1);
    assertEquals(meter.getCount(), 1);
  }

  @Test
  public void testGauge() throws Exception {
    Gauge<Long> gauge = factory.gauge().register("greetingService", "sayHello", new Gauge<Long>() {
      @Override
      public Long getValue() {
        return 1L;
      }
    });
    assertEquals(gauge.getValue().longValue(), 1L);
  }
}
