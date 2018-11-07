package io.scalecube.benchmarks.transport.codec.protostuff;

import io.scalecube.benchmarks.transport.codec.SmCodecBenchmarkState;
import io.scalecube.benchmarks.transport.codec.SmPartialEncodeScenario;

public class ProtostuffSmPartialEncodeBenchmark {

  /**
   * Main method.
   *
   * @param args - params of main method.
   */
  public static void main(String[] args) {
    SmPartialEncodeScenario.runWith(args, SmCodecBenchmarkState.Protostuff::new);
  }
}
