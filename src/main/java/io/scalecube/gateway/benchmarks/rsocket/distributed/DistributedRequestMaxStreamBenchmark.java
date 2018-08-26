package io.scalecube.gateway.benchmarks.rsocket.distributed;

import io.scalecube.gateway.benchmarks.RequestMaxStreamBenchmark;

public class DistributedRequestMaxStreamBenchmark {

  public static void main(String[] args) {
    RequestMaxStreamBenchmark.runWith(args, DistributedMicrobenchmarkState::new);
  }
}
