package io.scalecube.gateway.benchmarks.rsocket.remote;

import io.scalecube.gateway.benchmarks.RequestStreamBenchmark;

public class RemoteRequestStreamBenchmark {

  public static void main(String[] args) {
    RequestStreamBenchmark.runWith(args, RemoteBenchmarkState::new);
  }
}
