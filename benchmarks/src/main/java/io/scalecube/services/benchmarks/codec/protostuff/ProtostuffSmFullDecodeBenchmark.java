package io.scalecube.services.benchmarks.codec.protostuff;

import io.scalecube.services.benchmarks.codec.SmCodecBenchmarkState;
import io.scalecube.services.benchmarks.codec.SmFullDecodeScenario;

public class ProtostuffSmFullDecodeBenchmark {

  /**
   * Main method.
   *
   * @param args - params of main method.
   */
  public static void main(String[] args) {
    SmFullDecodeScenario.runWith(args, SmCodecBenchmarkState.Protostuff::new);
  }
}
