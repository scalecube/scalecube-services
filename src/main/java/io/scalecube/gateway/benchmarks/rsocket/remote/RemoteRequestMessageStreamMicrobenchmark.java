package io.scalecube.gateway.benchmarks.rsocket.remote;

import io.scalecube.gateway.benchmarks.RequestMessageStreamBenchmark;

public class RemoteRequestMessageStreamMicrobenchmark {

  /**
   * Main runner.
   *
   * @param args program arguments
   */
  public static void main(String[] args) {
    RequestMessageStreamBenchmark.runWith(args, RemoteBenchmarkState::new);
  }
}
