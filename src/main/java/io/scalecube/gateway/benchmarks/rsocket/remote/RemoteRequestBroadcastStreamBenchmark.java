package io.scalecube.gateway.benchmarks.rsocket.remote;

import io.scalecube.gateway.benchmarks.RequestBroadcastStreamBenchmark;

public class RemoteRequestBroadcastStreamBenchmark {

  public static void main(String[] args) {
    RequestBroadcastStreamBenchmark.runWith(args, RemoteBenchmarkState::new);
  }
}
