package io.scalecube.gateway.benchmarks.rsocket.distributed;

import io.scalecube.gateway.benchmarks.RequestOneMessageBenchmark;

public class DistributedRequestOneMessageMicrobenchmark {

  /**
   * Main runner.
   *
   * @param args program arguments
   */
  public static void main(String[] args) {
    RequestOneMessageBenchmark.runWith(args, DistributedMicrobenchmarkState::new);
  }
}
