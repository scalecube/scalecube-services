package io.scalecube.benchmarks.transport.codec.protostuff;

import io.scalecube.benchmarks.transport.codec.SmCodecBenchmarkState;
import io.scalecube.benchmarks.transport.codec.SmFullDecodeScenario;

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
