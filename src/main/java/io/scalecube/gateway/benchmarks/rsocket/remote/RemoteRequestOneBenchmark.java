package io.scalecube.gateway.benchmarks.rsocket.remote;

import io.scalecube.gateway.benchmarks.RequestOneBenchmark;

public class RemoteRequestOneBenchmark {

  public static void main(String[] args) {
    RequestOneBenchmark.runWith(args, RemoteBenchmarkState::new);
  }
}
