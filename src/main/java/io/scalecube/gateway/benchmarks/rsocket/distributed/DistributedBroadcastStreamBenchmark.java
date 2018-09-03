package io.scalecube.gateway.benchmarks.rsocket.distributed;

import io.scalecube.gateway.benchmarks.BroadcastStreamBenchmark;

public class DistributedBroadcastStreamBenchmark {

  public static void main(String[] args) {
    BroadcastStreamBenchmark.runWith(args, DistributedMicrobenchmarkState::new);
  }
}
