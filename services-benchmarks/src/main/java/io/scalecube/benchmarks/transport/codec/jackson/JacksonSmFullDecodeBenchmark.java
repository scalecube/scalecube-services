package io.scalecube.benchmarks.transport.codec.jackson;

import io.scalecube.benchmarks.transport.codec.SmCodecBenchmarkState;
import io.scalecube.benchmarks.transport.codec.SmFullDecodeScenario;

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
