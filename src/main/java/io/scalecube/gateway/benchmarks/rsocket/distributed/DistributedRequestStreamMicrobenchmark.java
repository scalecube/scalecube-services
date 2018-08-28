package io.scalecube.gateway.benchmarks.rsocket.distributed;

import io.scalecube.gateway.benchmarks.RequestStreamBenchmark;

public class DistributedRequestStreamMicrobenchmark {

  /**
   * Main runner.
   *
   * @param args program arguments
   */
  public static void main(String[] args) {
    RequestStreamBenchmark.runWith(args, DistributedMicrobenchmarkState::new);
  }
}
