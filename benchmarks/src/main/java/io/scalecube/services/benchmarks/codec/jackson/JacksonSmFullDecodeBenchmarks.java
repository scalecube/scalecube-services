package io.scalecube.services.benchmarks.codec.jackson;

import io.scalecube.services.benchmarks.codec.SmCodecBenchmarksState;
import io.scalecube.services.benchmarks.codec.SmFullDecodeBenchmarks;

public class JacksonSmFullDecodeBenchmarks {

  /**
   * Main method.
   *
   * @param args - params of main method.
   */
  public static void main(String[] args) {
    SmFullDecodeBenchmarks.runWith(args, SmCodecBenchmarksState.Jackson::new);
  }
}
