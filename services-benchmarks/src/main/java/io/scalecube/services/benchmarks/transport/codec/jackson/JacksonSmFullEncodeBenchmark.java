package io.scalecube.services.benchmarks.transport.codec.jackson;

import io.scalecube.services.benchmarks.transport.codec.SmCodecBenchmarkState;
import io.scalecube.services.benchmarks.transport.codec.SmFullEncodeScenario;

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
