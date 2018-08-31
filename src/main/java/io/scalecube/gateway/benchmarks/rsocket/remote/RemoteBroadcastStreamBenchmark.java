package io.scalecube.gateway.benchmarks.rsocket.remote;

import io.scalecube.gateway.benchmarks.BroadcastStreamBenchmark;

public class RemoteBroadcastStreamBenchmark {

  public static void main(String[] args) {
    BroadcastStreamBenchmark.runWith(args, RemoteBenchmarksState::new);
  }
}
