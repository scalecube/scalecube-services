package io.scalecube.gateway.benchmarks.rsocket.standalone;

import io.scalecube.gateway.benchmarks.InfiniteStreamBenchmark;

public class StandaloneInfiniteStreamBenchmark {

  /**
   * Main runner.
   *
   * @param args program arguments
   */
  public static void main(String[] args) {
    InfiniteStreamBenchmark.runWith(args, StandaloneMicrobenchmarkState::new);
  }
}
