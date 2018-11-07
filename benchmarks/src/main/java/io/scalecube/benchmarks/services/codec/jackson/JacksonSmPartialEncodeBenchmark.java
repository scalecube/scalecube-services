package io.scalecube.benchmarks.services.codec.jackson;

import io.scalecube.benchmarks.services.codec.SmCodecBenchmarkState;
import io.scalecube.benchmarks.services.codec.SmPartialEncodeScenario;

public class JacksonSmPartialEncodeBenchmark {

  /**
   * Main method.
   *
   * @param args - params of main method.
   */
  public static void main(String[] args) {
    SmPartialEncodeScenario.runWith(args, SmCodecBenchmarkState.Jackson::new);
  }
}
