package io.scalecube.gateway.benchmarks.rsocket.distributed;

import io.scalecube.gateway.benchmarks.RequestOneBenchmark;

public class DistributedRequestOneMicrobenchmark {

  public static void main(String[] args) {
    RequestOneBenchmark.runWith(args, DistributedMicrobenchmarkState::new);
  }
}
