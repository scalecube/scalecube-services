package io.scalecube.gateway.benchmarks.rsocket.standalone;

import io.scalecube.gateway.benchmarks.RequestBroadcastStreamBenchmark;

public class StandaloneRequestBroadcastStreamMicrobenchmark {

  public static void main(String[] args) {
    RequestBroadcastStreamBenchmark.runWith(args, StandaloneMicrobenchmarkState::new);
  }
}
