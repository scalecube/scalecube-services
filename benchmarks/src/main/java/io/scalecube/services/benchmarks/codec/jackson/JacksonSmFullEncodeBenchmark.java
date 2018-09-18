package io.scalecube.services.benchmarks.codec.jackson;

import io.scalecube.services.benchmarks.codec.SmCodecBenchmarkState;
import io.scalecube.services.benchmarks.codec.SmFullEncodeScenario;

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
