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

  // TODO [AK]: Create class NetworkEmulator and move defauls and part of other logic there
  private static NetworkEmulatorSettings defaultSettings = new NetworkEmulatorSettings(0, 0);

  public static NetworkEmulatorSettings defaultSettings() {
    return defaultSettings;
  }

  public static void setDefaultSettings(int lostPercent, int delay) {
    defaultSettings = new NetworkEmulatorSettings(lostPercent, delay);
  }

  private final int lostPercent;

  private final int mean;

  public NetworkEmulatorSettings(int lostPercent, int mean) {
    this.lostPercent = lostPercent;
    this.mean = mean;
  }

  /** Probability of message loss in percents. */
  public int getLostPercent() {
    return lostPercent;
  }

  /** Mean network delay for message in milliseconds. */
  public int getMean() {
    return mean;
  }

  public boolean evaluateLost() {
    return lostPercent > 0 && (lostPercent >= 100 || ThreadLocalRandom.current().nextInt(100) < lostPercent);
  }

  /** Delays are emulated using exponential distribution of probabilities. */
  public long evaluateDelay() {
    if (mean > 0) {
      // Network delays (network delays). Delays should be emulated using exponential distribution of probabilities.
      // log(1-x)/(1/mean)
      Double x0 = ThreadLocalRandom.current().nextDouble();
      Double y0 = -Math.log(1 - x0) * mean;
      return y0.longValue();
    }
    return 0;
  }

  @Override
  public String toString() {
    return "NetworkEmulatorSettings{" + "lostPercent=" + lostPercent + ", mean=" + mean + '}';
  }
}
