package io.scalecube.benchmarks.services.codec.protostuff;

import io.scalecube.benchmarks.services.codec.SmCodecBenchmarkState;
import io.scalecube.benchmarks.services.codec.SmFullDecodeScenario;

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
