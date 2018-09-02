package io.scalecube.services.benchmarks.codec.protostuff;

import io.scalecube.services.benchmarks.codec.SmCodecBenchmarksState;
import io.scalecube.services.benchmarks.codec.SmFullDecodeBenchmarks;

public class ProtostuffSmFullDecodeBenchmarks {

  /**
   * Main method.
   *
   * @param args - params of main method.
   */
  public static void main(String[] args) {
    SmFullDecodeBenchmarks.runWith(args, SmCodecBenchmarksState.Protostuff::new);
  }
}
