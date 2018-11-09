package io.scalecube.services.benchmarks.transport.codec.protostuff;

import io.scalecube.services.benchmarks.transport.codec.SmCodecBenchmarkState;
import io.scalecube.services.benchmarks.transport.codec.SmFullDecodeScenario;

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
