package io.scalecube.benchmarks.services.codec.jackson;

import io.scalecube.benchmarks.services.codec.SmCodecBenchmarkState;
import io.scalecube.benchmarks.services.codec.SmFullEncodeScenario;

public class JacksonSmFullEncodeBenchmark {

  /**
   * Main method.
   *
   * @param args - params of main method.
   */
  public static void main(String[] args) {
    SmFullEncodeScenario.runWith(args, SmCodecBenchmarkState.Jackson::new);
  }
}
