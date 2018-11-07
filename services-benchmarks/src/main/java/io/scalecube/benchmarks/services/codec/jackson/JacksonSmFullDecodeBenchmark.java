package io.scalecube.benchmarks.services.codec.jackson;

import io.scalecube.benchmarks.services.codec.SmCodecBenchmarkState;
import io.scalecube.benchmarks.services.codec.SmFullDecodeScenario;

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
