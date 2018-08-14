package io.scalecube.gateway.benchmarks.rsocket.distributed;

import io.scalecube.gateway.benchmarks.RequestStreamBenchmark;

public class DistributedRequestStreamMicrobenchmark {

  public static void main(String[] args) {
    RequestStreamBenchmark.runWith(args, DistributedMicrobenchmarkState::new);
  }
}
