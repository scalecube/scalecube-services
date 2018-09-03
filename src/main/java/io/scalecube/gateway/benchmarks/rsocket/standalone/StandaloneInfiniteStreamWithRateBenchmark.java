package io.scalecube.gateway.benchmarks.rsocket.standalone;

import io.scalecube.gateway.benchmarks.InfiniteStreamWithRateBenchmark;

public class StandaloneInfiniteStreamWithRateBenchmark {

  /**
   * Main runner.
   *
   * @param args program arguments
   */
  public static void main(String[] args) {
    InfiniteStreamWithRateBenchmark.runWith(args, StandaloneMicrobenchmarkState::new);
  }
}
