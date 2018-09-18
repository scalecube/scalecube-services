package io.scalecube.services.benchmarks.codec.jackson;

import io.scalecube.services.benchmarks.codec.SmCodecBenchmarkState;
import io.scalecube.services.benchmarks.codec.SmPartialDecodeScenario;

public class JacksonSmPartialDecodeBenchmark {

  /**
   * Main method.
   *
   * @param args - params of main method.
   */
  public static void main(String[] args) {
    SmPartialDecodeScenario.runWith(args, SmCodecBenchmarkState.Jackson::new);
  }
}
