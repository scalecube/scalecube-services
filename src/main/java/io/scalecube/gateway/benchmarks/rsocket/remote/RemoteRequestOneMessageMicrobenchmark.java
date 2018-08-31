package io.scalecube.gateway.benchmarks.rsocket.remote;

import io.scalecube.gateway.benchmarks.RequestOneMessageBenchmark;

public class RemoteRequestOneMessageMicrobenchmark {

  /**
   * Main runner.
   *
   * @param args program arguments
   */
  public static void main(String[] args) {
    RequestOneMessageBenchmark.runWith(args, RemoteBenchmarkState::new);
  }
}
