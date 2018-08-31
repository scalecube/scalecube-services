package io.scalecube.gateway.benchmarks.rsocket.remote;

import io.scalecube.gateway.benchmarks.RequestMaxStreamBenchmark;

public class RemoteRequestMaxStreamBenchmark {

  /**
   * Main runner.
   *
   * @param args program arguments
   */
  public static void main(String[] args) {
    RequestMaxStreamBenchmark.runWith(args, RemoteBenchmarkState::new);
  }
}
