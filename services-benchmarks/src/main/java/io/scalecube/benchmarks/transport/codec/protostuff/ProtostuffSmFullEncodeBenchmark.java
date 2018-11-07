package io.scalecube.benchmarks.transport.codec.protostuff;

import io.scalecube.benchmarks.transport.codec.SmCodecBenchmarkState;
import io.scalecube.benchmarks.transport.codec.SmFullEncodeScenario;

public class ProtostuffSmFullEncodeBenchmark {

  /**
   * Main method.
   *
   * @param args - params of main method.
   */
  public static void main(String[] args) {
    SmFullEncodeScenario.runWith(args, SmCodecBenchmarkState.Protostuff::new);
  }
}
