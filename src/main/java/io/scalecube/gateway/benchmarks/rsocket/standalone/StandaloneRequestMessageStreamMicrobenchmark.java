package io.scalecube.gateway.benchmarks.rsocket.standalone;

import io.scalecube.gateway.benchmarks.RequestMessageStreamBenchmark;

public class StandaloneRequestMessageStreamMicrobenchmark {

  /**
   * Main runner.
   *
   * @param args program arguments
   */
  public static void main(String[] args) {
    RequestMessageStreamBenchmark.runWith(args, StandaloneMicrobenchmarkState::new);
  }
}
