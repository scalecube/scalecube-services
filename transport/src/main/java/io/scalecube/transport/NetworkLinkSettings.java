package io.scalecube.transport;

import java.util.concurrent.ThreadLocalRandom;

/**
 * This class contains settings for the network link and computations to evaluate message loss and message delay.
 * Following parameters is present:
 * <ul>
 * <li>Percent of losing messages</li>
 * <li>Mean network delays in milliseconds. Delays are emulated using exponential distribution of probabilities</li>
 * </ul>
 * 
 * @author Anton Kharenko
 */
public final class NetworkLinkSettings {

  private final int lossPercent;
  private final int meanDelay;

  public NetworkLinkSettings(int lossPercent, int mean) {
    this.lossPercent = lossPercent;
    this.meanDelay = mean;
  }

  /** Probability of message loss in percents. */
  public int lossPercent() {
    return lossPercent;
  }

  /** Mean network delay for message in milliseconds. */
  public int meanDelay() {
    return meanDelay;
  }

  public boolean evaluateLoss() {
    return lossPercent > 0 && (lossPercent >= 100 || ThreadLocalRandom.current().nextInt(100) < lossPercent);
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
    return "NetworkLinkSettings{lossPercent=" + lossPercent + ", meanDelay=" + meanDelay + '}';
  }
}
