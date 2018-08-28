package io.scalecube.gateway.benchmarks.rsocket.standalone;

import io.scalecube.gateway.benchmarks.RequestRawBroadcastStreamBenchmark;

public class StandaloneRequestRawBroadcastStreamMicrobenchmark {

  public static void main(String[] args) {
    RequestRawBroadcastStreamBenchmark.runWith(args, StandaloneMicrobenchmarkState::new);
  }
}
