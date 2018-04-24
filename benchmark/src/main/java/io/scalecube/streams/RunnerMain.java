package io.scalecube.streams;

import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class RunnerMain {

  public static void main(String[] args) throws RunnerException {
    Options opts = new OptionsBuilder()
        .include(BenchmarkTest.class.getSimpleName())
        .warmupIterations(10)
        .measurementIterations(10)
        .forks(1)
        .build();

    new Runner(opts).run();
  }

}
