package io.scalecube.gateway.benchmarks.rsocket.distributed;

import io.scalecube.gateway.benchmarks.InfiniteStreamWithRateBenchmark;

public class DistributedInfiniteStreamWithRateBenchmark {

  /**
   * Main runner.
   *
   * @param args program arguments
   */
  public static void main(String[] args) {
    InfiniteStreamWithRateBenchmark.runWith(args, DistributedMicrobenchmarkState::new);
  }
}
