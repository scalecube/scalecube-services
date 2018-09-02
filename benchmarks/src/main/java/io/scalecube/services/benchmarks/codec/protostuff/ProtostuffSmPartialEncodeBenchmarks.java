package io.scalecube.services.benchmarks.codec.protostuff;

import io.scalecube.services.benchmarks.codec.SmCodecBenchmarksState;
import io.scalecube.services.benchmarks.codec.SmPartialEncodeBenchmarks;

public class ProtostuffSmPartialEncodeBenchmarks {

  /**
   * Main method.
   *
   * @param args - params of main method.
   */
  public static void main(String[] args) {
    SmPartialEncodeBenchmarks.runWith(args, SmCodecBenchmarksState.Protostuff::new);
  }
}
