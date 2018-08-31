package io.scalecube.gateway.benchmarks.rsocket.remote;

import io.scalecube.gateway.benchmarks.InfiniteStreamWithRateBenchmark;

public class RemoteInfiniteStreamWithRateBenchmark {

  /**
   * Main runner.
   *
   * @param args program arguments
   */
  public static void main(String[] args) {
    InfiniteStreamWithRateBenchmark.runWith(args, RemoteBenchmarksState::new);
  }
}
