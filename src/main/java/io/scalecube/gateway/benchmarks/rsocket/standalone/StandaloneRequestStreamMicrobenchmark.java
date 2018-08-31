package io.scalecube.gateway.benchmarks.rsocket.standalone;

import io.scalecube.gateway.benchmarks.RequestStreamBenchmark;

public class StandaloneRequestStreamMicrobenchmark {

  /**
   * Main runner.
   *
   * @param args program arguments
   */
  public static void main(String[] args) {
    RequestStreamBenchmark.runWith(args, StandaloneMicrobenchmarkState::new);
  }
}
