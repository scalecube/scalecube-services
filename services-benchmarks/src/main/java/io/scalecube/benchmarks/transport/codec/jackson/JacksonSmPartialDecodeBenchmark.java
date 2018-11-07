package io.scalecube.benchmarks.transport.codec.jackson;

import io.scalecube.benchmarks.transport.codec.SmCodecBenchmarkState;
import io.scalecube.benchmarks.transport.codec.SmPartialDecodeScenario;

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
