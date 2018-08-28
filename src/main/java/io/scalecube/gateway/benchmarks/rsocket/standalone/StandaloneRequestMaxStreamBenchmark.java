package io.scalecube.gateway.benchmarks.rsocket.standalone;

import io.scalecube.gateway.benchmarks.RequestMaxStreamBenchmark;

public class StandaloneRequestMaxStreamBenchmark {

  /**
   * Main runner.
   *
   * @param args program arguments
   */
  public static void main(String[] args) {
    RequestMaxStreamBenchmark.runWith(args, StandaloneMicrobenchmarkState::new);
  }
}
