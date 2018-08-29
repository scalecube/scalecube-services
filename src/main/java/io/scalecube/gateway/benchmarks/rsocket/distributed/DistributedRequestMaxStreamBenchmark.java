package io.scalecube.gateway.benchmarks.rsocket.distributed;

import io.scalecube.gateway.benchmarks.RequestMaxStreamBenchmark;

public class DistributedRequestMaxStreamBenchmark {

  /**
   * Main runner.
   *
   * @param args program arguments
   */
  public static void main(String[] args) {
    RequestMaxStreamBenchmark.runWith(args, DistributedMicrobenchmarkState::new);
  }
}
