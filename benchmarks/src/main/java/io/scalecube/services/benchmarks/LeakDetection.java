package io.scalecube.services.benchmarks;

/**
 * Helper utility to track down buffer leaks.
 */
public class LeakDetection {
  static {
    System.setProperty("io.netty.leakDetection.level", "paranoid");
  }

  public static void main(String[] args) {
    ServicesBenchmarksState state = new ServicesBenchmarksState();
    state.setup();
    for (int i = 0; i < 1e3; i++) {
      state.service().fireAndForget0().block();
    }
  }
}
