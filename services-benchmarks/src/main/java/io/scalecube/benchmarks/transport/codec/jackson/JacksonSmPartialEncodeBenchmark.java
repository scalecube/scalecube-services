package io.scalecube.benchmarks.transport.codec.jackson;

import io.scalecube.benchmarks.transport.codec.SmCodecBenchmarkState;
import io.scalecube.benchmarks.transport.codec.SmPartialEncodeScenario;

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
