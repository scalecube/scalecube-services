package io.scalecube.services.benchmarks.codec.jackson;

import io.scalecube.services.benchmarks.codec.SmCodecBenchmarksState;
import io.scalecube.services.benchmarks.codec.SmFullEncodeBenchmarks;

public class JacksonSmFullEncodeBenchmarks {

  /**
   * Main method.
   *
   * @param args - params of main method.
   */
  public static void main(String[] args) {
    SmFullEncodeBenchmarks.runWith(args, SmCodecBenchmarksState.Jackson::new);
  }
}
