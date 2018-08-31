package io.scalecube.gateway.benchmarks.rsocket.remote;

import io.scalecube.gateway.benchmarks.RequestRawBroadcastStreamBenchmark;

public class RemoteRequestRawBroadcastStreamBenchmark {

  public static void main(String[] args) {
    RequestRawBroadcastStreamBenchmark.runWith(args, RemoteBenchmarkState::new);
  }
}
