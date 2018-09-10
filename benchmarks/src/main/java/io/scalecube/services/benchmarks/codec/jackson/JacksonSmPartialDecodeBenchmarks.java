package io.scalecube.services.benchmarks.codec.jackson;

import io.scalecube.services.benchmarks.codec.SmCodecBenchmarksState;
import io.scalecube.services.benchmarks.codec.SmPartialDecodeBenchmarks;

public class JacksonSmPartialDecodeBenchmarks {

  /**
   * Main method.
   *
   * @param args - params of main method.
   */
  public static void main(String[] args) {
    SmPartialDecodeBenchmarks.runWith(args, SmCodecBenchmarksState.Jackson::new);
  }
}
