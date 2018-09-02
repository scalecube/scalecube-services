package io.scalecube.services.benchmarks.codec.protostuff;

import io.scalecube.services.benchmarks.codec.SmCodecBenchmarksState;
import io.scalecube.services.benchmarks.codec.SmPartialDecodeBenchmarks;

public class ProtostuffSmPartialDecodeBenchmarks {

  /**
   * Main method.
   *
   * @param args - params of main method.
   */
  public static void main(String[] args) {
    SmPartialDecodeBenchmarks.runWith(args, SmCodecBenchmarksState.Protostuff::new);
  }
}
