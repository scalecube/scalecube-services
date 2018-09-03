package io.scalecube.gateway.benchmarks.rsocket.standalone;

import io.scalecube.gateway.benchmarks.BroadcastStreamBenchmark;

public class StandaloneBroadcastStreamBenchmark {

  public static void main(String[] args) {
    BroadcastStreamBenchmark.runWith(args, StandaloneMicrobenchmarkState::new);
  }
}
