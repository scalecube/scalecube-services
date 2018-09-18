package io.scalecube.services.benchmarks.codec.jackson;

import io.scalecube.services.benchmarks.codec.SmCodecBenchmarkState;
import io.scalecube.services.benchmarks.codec.SmFullDecodeScenario;

public class JacksonSmFullDecodeBenchmark {

  /**
   * Main method.
   *
   * @param args - params of main method.
   */
  public static void main(String[] args) {
    SmFullDecodeScenario.runWith(args, SmCodecBenchmarkState.Jackson::new);
  }
}
