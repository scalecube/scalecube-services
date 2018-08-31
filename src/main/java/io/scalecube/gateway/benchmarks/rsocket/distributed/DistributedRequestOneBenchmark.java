package io.scalecube.gateway.benchmarks.rsocket.distributed;

import io.scalecube.gateway.benchmarks.RequestOneBenchmark;

public class DistributedRequestOneBenchmark {

  /**
   * Main runner.
   *
   * @param args program arguments
   */
  public static void main(String[] args) {
    RequestOneBenchmark.runWith(args, DistributedMicrobenchmarkState::new);
  }
}
