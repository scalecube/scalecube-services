package io.scalecube.gateway.benchmarks.rsocket.remote;

import io.scalecube.gateway.benchmarks.rsocket.RequestMaxStreamBenchmark;

public class RemoteRequestMaxStreamBenchmark {

  public static void main(String[] args) {
    RequestMaxStreamBenchmark.runWith(args, RemoteBenchmarkState::new);
  }
}
