package io.scalecube.gateway.benchmarks.rsocket.standalone;

import io.scalecube.gateway.benchmarks.RequestStreamBenchmark;

public class StandaloneRequestStreamMicrobenchmark {

  public static void main(String[] args) {
    RequestStreamBenchmark.runWith(args, StandaloneMicrobenchmarkState::new);
  }
}
