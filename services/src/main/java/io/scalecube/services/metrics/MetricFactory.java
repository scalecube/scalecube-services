package io.scalecube.services.metrics;

public interface MetricFactory {

  MeterFactory meter();

  TimerFactory timer();

  CounterFactory counter();

  GaugeFactory gauge();

  HistogramFactory histogram();

}
