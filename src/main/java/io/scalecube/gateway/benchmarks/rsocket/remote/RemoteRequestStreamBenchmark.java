package io.scalecube.gateway.benchmarks.rsocket.remote;

import io.scalecube.gateway.benchmarks.RequestStreamBenchmark;

public class RemoteRequestStreamBenchmark {

  /**
   * Main runner.
   *
   * @param args program arguments
   */
  public static void main(String[] args) {
    RequestStreamBenchmark.runWith(args, RemoteBenchmarkState::new);
  }
}
