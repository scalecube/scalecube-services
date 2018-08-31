package io.scalecube.gateway.benchmarks.rsocket.distributed;

import io.scalecube.gateway.benchmarks.RequestMessageStreamBenchmark;

public class DistributedRequestMessageStreamMicrobenchmark {

  /**
   * Main runner.
   *
   * @param args program arguments
   */
  public static void main(String[] args) {
    RequestMessageStreamBenchmark.runWith(args, DistributedMicrobenchmarkState::new);
  }
}
