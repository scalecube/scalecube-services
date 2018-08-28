package io.scalecube.gateway.benchmarks.rsocket.distributed;

import io.scalecube.gateway.benchmarks.RequestRawBroadcastStreamBenchmark;

public class DistributedRequestRawBroadcastStreamBenchmark {

  public static void main(String[] args) {
    RequestRawBroadcastStreamBenchmark.runWith(args, DistributedMicrobenchmarkState::new);
  }
}
