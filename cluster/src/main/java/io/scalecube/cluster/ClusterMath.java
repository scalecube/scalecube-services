package io.scalecube.cluster;

/**
 * Utility class which contains math computation on cluster properties.
 *
 * @author Anton Kharenko
 */
public final class ClusterMath {

  private ClusterMath() {
    // Do not instantiate
  }

  public static double gossipConvergencePercent(int fanout, int repeatMult, int clusterSize, double lossPercent) {
    double msgLossProb = lossPercent / 100.0;
    return gossipConvergenceProbability(fanout, repeatMult, clusterSize, msgLossProb) * 100;
  }

  public static double gossipConvergenceProbability(int fanout, int repeatMult, int clusterSize, double loss) {
    double fanoutWithLoss = (1.0 - loss) * fanout;
    double spreadSize = clusterSize - Math.pow(clusterSize, -(fanoutWithLoss * repeatMult - 2));
    return spreadSize / clusterSize;
  }

  public static int maxMessagesPerGossipTotal(int fanout, int repeatMult, int clusterSize) {
    return clusterSize * maxMessagesPerGossipPerNode(fanout, repeatMult, clusterSize);
  }

  public static int maxMessagesPerGossipPerNode(int fanout, int repeatMult, int clusterSize) {
    return fanout * repeatMult * ceilLog2(clusterSize);
  }

  public static long gossipDisseminationTime(int repeatMult, int clusterSize, long gossipInterval) {
    return gossipPeriodsToSpread(repeatMult, clusterSize) * gossipInterval;
  }

  public static long gossipTimeoutToSweep(int repeatMult, int clusterSize, long gossipInterval) {
    return gossipPeriodsToSweep(repeatMult, clusterSize) * gossipInterval;
  }

  public static int gossipPeriodsToSweep(int repeatMult, int clusterSize) {
    int periodsToSpread = gossipPeriodsToSpread(repeatMult, clusterSize);
    return 2 * (periodsToSpread + 1);
  }

  public static int gossipPeriodsToSpread(int repeatMult, int clusterSize) {
    return repeatMult * ceilLog2(clusterSize);
  }

  public static long suspicionTimeout(int suspicionMult, int clusterSize, long pingInterval) {
    return suspicionMult * ceilLog2(clusterSize) * pingInterval;
  }

  /**
   * Returns ceil(log2(n + 1)).
   */
  public static int ceilLog2(int num) {
    return 32 - Integer.numberOfLeadingZeros(num);
  }
}
