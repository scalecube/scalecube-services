package io.scalecube.gateway.benchmarks.rsocket.distributed;

import io.scalecube.gateway.benchmarks.RequestBroadcastStreamBenchmark;

public class DistributedRequestBroadcastStreamBenchmark {

  public static void main(String[] args) {
    RequestBroadcastStreamBenchmark.runWith(args, DistributedMicrobenchmarkState::new);
  }
}
