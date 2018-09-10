package io.scalecube.services.benchmarks.codec.jackson;

import io.scalecube.services.benchmarks.codec.SmCodecBenchmarksState;
import io.scalecube.services.benchmarks.codec.SmPartialEncodeBenchmarks;

public class JacksonSmPartialEncodeBenchmarks {

  /**
   * Main method.
   *
   * @param args - params of main method.
   */
  public static void main(String[] args) {
    SmPartialEncodeBenchmarks.runWith(args, SmCodecBenchmarksState.Jackson::new);
  }
}
