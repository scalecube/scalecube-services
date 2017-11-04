package io.scalecube.metrics.codahale;

import static org.junit.Assert.assertEquals;

import io.scalecube.metrics.api.Counter;
import io.scalecube.metrics.api.MetricFactory;
import io.scalecube.testlib.BaseTest;

import com.codahale.metrics.MetricRegistry;

import org.junit.Test;

public class CodahaleMetricsFactoryTest extends BaseTest{

  @Test
  public void testCreateCounter() throws Exception {
   
    MetricRegistry metrics = new MetricRegistry();
    
    MetricFactory factory = new CodahaleMetricsFactory(metrics);
    
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
}
