package io.scalecube.transport;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Tuple class. Contains:
 * <ul>
 * <li>Percent of losing messages</li>
 * <li>Network delays in milliseconds. Delays should be emulated using exponential distribution of probabilities</li>
 * </ul>
 * 
 * @author alexeyz
 */
public class NetworkEmulatorSettings {

  private final int lostPercent;
  private final int meanDelay;

  public NetworkEmulatorSettings(int lostPercent, int mean) {
    this.lostPercent = lostPercent;
    this.meanDelay = mean;
  }

  /** Probability of message loss in percents. */
  public int getLostPercent() {
    return lostPercent;
  }

  /** Mean network delay for message in milliseconds. */
  public int getMeanDelay() {
    return meanDelay;
  }

  public boolean evaluateLost() {
    return lostPercent > 0 && (lostPercent >= 100 || ThreadLocalRandom.current().nextInt(100) < lostPercent);
  }

  /** Delays are emulated using exponential distribution of probabilities. */
  public long evaluateDelay() {
    if (meanDelay > 0) {
      // Network delays (network delays). Delays should be emulated using exponential distribution of probabilities.
      // log(1-x)/(1/mean)
      Double x0 = ThreadLocalRandom.current().nextDouble();
      Double y0 = -Math.log(1 - x0) * meanDelay;
      return y0.longValue();
    }
    return 0;
  }

  @Override
  public String toString() {
    return "NetworkEmulatorSettings{lostPercent=" + lostPercent + ", meanDelay=" + meanDelay + '}';
  }
}
