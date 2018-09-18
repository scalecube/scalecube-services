package io.scalecube.services.benchmarks.codec.jackson;

import io.scalecube.services.benchmarks.codec.SmCodecBenchmarkState;
import io.scalecube.services.benchmarks.codec.SmPartialEncodeScenario;

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
