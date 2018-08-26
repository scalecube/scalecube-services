package io.scalecube.gateway.benchmarks.rsocket.standalone;

import io.scalecube.gateway.benchmarks.RequestMaxStreamBenchmark;

public class StandaloneRequestMaxStreamBenchmark {

  public static void main(String[] args) {
    RequestMaxStreamBenchmark.runWith(args, StandaloneMicrobenchmarkState::new);
  }
}
