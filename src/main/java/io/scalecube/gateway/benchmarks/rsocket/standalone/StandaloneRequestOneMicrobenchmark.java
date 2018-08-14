package io.scalecube.gateway.benchmarks.rsocket.standalone;

import io.scalecube.gateway.benchmarks.RequestOneBenchmark;

public class StandaloneRequestOneMicrobenchmark {

  public static void main(String[] args) {
    RequestOneBenchmark.runWith(args, StandaloneMicrobenchmarkState::new);
  }
}
