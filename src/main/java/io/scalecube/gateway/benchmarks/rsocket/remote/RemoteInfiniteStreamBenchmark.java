package io.scalecube.gateway.benchmarks.rsocket.remote;

import io.scalecube.gateway.benchmarks.InfiniteStreamBenchmark;

public class RemoteInfiniteStreamBenchmark {

  /**
   * Main runner.
   *
   * @param args program arguments
   */
  public static void main(String[] args) {
    InfiniteStreamBenchmark.runWith(args, RemoteBenchmarksState::new);
  }
}
