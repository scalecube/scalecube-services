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

  /**
   * Constructor for link settings.
   * 
   * @param lossPercent loss in percent
   * @param mean mean dealy
   */
  public NetworkLinkSettings(int lossPercent, int mean) {
    this.lossPercent = lossPercent;
    this.meanDelay = mean;
  }

  /**
   * Returns probability of message loss in percents.
   * 
   * @return loss in percents
   */
  public int lossPercent() {
    return lossPercent;
  }

  /**
   * Returns mean network delay for message in milliseconds.
   * 
   * @return mean delay
   */
  public int meanDelay() {
    return meanDelay;
  }

  /**
   * Indicator function telling is loss enabled.
   *
   * @return boolean indicating would loss occur
   */
  public boolean evaluateLoss() {
    return lossPercent > 0 && (lossPercent >= 100 || ThreadLocalRandom.current().nextInt(100) < lossPercent);
  }

  /**
   * Evaluates network delay according to exponential distribution of probabilities.
   * 
   * @return delay
   */
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
