package io.servicefabric.transport;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Tuple class. Contains:
 * <ul>
 *  <li>Percent of losing messages</li>
 *  <li>Network delays in milliseconds. Delays should be emulated using exponential distribution of probabilities</li>
 *  </ul>
 *  @author alexeyz
 */
public class NetworkEmulatorSettings {
	private static NetworkEmulatorSettings defaultSettings = new NetworkEmulatorSettings(0, 0);

	/** Percent of losing messages. */
	private final int lostPercent;
	/** Network delays in milliseconds. Delays should be emulated using exponential distribution of probabilities. */
	private final int mean;

	public NetworkEmulatorSettings(int lostPercent, int mean) {
		this.lostPercent = lostPercent;
		this.mean = mean;
	}

	public int getLostPercent() {
		return lostPercent;
	}

	public int getMean() {
		return mean;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("NetworkSettings [lostPercent=");
		builder.append(lostPercent);
		builder.append(", mean=");
		builder.append(mean);
		builder.append("]");
		return builder.toString();
	}

	public static NetworkEmulatorSettings defaultSettings() {
		return defaultSettings;
	}

	public static void setDefaultSettings(int lostPercent, int delay) {
		defaultSettings = new NetworkEmulatorSettings(lostPercent, delay);
	}

	public boolean breakDueToNetwork() {
		return lostPercent > 0 && (lostPercent >= 100 || ThreadLocalRandom.current().nextInt(100) < lostPercent);
	}

	public long evaluateTimeToSleep() {
		if (mean > 0) {
			// Network delays (network delays). Delays should be emulated using exponential distribution of probabilities.
			//  log(1-x)/(1/mean)
			Double x = ThreadLocalRandom.current().nextDouble();
			Double y = -Math.log(1 - x) * mean;
			return y.longValue();
		}
		return 0;
	}
}
