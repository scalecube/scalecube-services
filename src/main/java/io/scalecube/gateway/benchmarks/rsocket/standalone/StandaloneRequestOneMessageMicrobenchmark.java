package io.scalecube.gateway.benchmarks.rsocket.standalone;

import io.scalecube.gateway.benchmarks.RequestOneMessageBenchmark;

public class StandaloneRequestOneMessageMicrobenchmark {

  /**
   * Main runner.
   *
   * @param args program arguments
   */
  public static void main(String[] args) {
    RequestOneMessageBenchmark.runWith(args, StandaloneMicrobenchmarkState::new);
  }
}
