package io.scalecube.services.benchmarks.transport.codec.protostuff;

import io.scalecube.services.benchmarks.transport.codec.SmCodecBenchmarkState;
import io.scalecube.services.benchmarks.transport.codec.SmFullEncodeScenario;

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
