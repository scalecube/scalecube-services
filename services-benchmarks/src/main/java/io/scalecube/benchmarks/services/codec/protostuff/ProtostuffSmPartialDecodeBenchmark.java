package io.scalecube.benchmarks.services.codec.protostuff;

import io.scalecube.benchmarks.services.codec.SmCodecBenchmarkState;
import io.scalecube.benchmarks.services.codec.SmPartialDecodeScenario;

public class ProtostuffSmPartialDecodeBenchmark {

  /**
   * Main method.
   *
   * @param args - params of main method.
   */
  public static void main(String[] args) {
    SmPartialDecodeScenario.runWith(args, SmCodecBenchmarkState.Protostuff::new);
  }
}
