package io.scalecube.transport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Network Emulator is allowing to control link quality between endpoints in order to allow testing of message loss,
 * message delay, cluster partitions cluster recovery and other network related conditions.
 *
 * @author Anton Kharenko
 */
public final class NetworkEmulator {

  private static final Logger LOGGER = LoggerFactory.getLogger(NetworkEmulator.class);

  public static final NetworkLinkSettings DEAD_LINK_SETTINGS = new NetworkLinkSettings(100, 0);
  public static final NetworkLinkSettings ALIVE_LINK_SETTINGS = new NetworkLinkSettings(0, 0);

  private volatile NetworkLinkSettings defaultLinkSettings = ALIVE_LINK_SETTINGS;

  private final Map<Address, NetworkLinkSettings> customLinkSettings = new ConcurrentHashMap<>();

  private final AtomicLong totalMessageSentCount = new AtomicLong();

  private final AtomicLong totalMessageLostCount = new AtomicLong();

  private final boolean enabled;

  private final Address address;

  /**
   * Creates new instance of network emulator. Should be always created internally by Transport.
   *
   * @param address local address
   * @param enabled either network emulator is enabled
   */
  NetworkEmulator(Address address, boolean enabled) {
    this.address = address;
    this.enabled = enabled;
  }

  /**
   * Returns link settings applied to the given destination.
   * 
   * @param destination address of target endpoint
   * @return network settings
   */
  public NetworkLinkSettings getLinkSettings(Address destination) {
    return customLinkSettings.getOrDefault(destination, defaultLinkSettings);
  }

  /**
   * Returns link settings applied to the given destination.
   * 
   * @param address socket address of target endpoint
   * @return network settings
   */
  public NetworkLinkSettings getLinkSettings(InetSocketAddress address) {
    // Check hostname:port
    Address address1 = Address.create(address.getHostName(), address.getPort());
    if (customLinkSettings.containsKey(address1)) {
      return customLinkSettings.get(address1);
    }

    // Check ip:port
    Address address2 = Address.create(address.getAddress().getHostAddress(), address.getPort());
    if (customLinkSettings.containsKey(address2)) {
      return customLinkSettings.get(address2);
    }

    // Use default
    return defaultLinkSettings;
  }

  /**
   * Sets given network emulator settings for specific link. If network emulator is disabled do nothing.
   * 
   * @param destination address of target endpoint
   * @param lossPercent loss in percents
   * @param meanDelay mean delay
   */
  public void setLinkSettings(Address destination, int lossPercent, int meanDelay) {
    if (!enabled) {
      LOGGER.warn("Can't set network settings (loss={}%, mean={}ms) from {} to {} since network emulator is disabled",
          lossPercent, meanDelay, address, destination);
      return;
    }
    NetworkLinkSettings settings = new NetworkLinkSettings(lossPercent, meanDelay);
    customLinkSettings.put(destination, settings);
    LOGGER.info("Set network settings (loss={}%, mean={}ms) from {} to {}",
        lossPercent, meanDelay, address, destination);
  }

  /**
   * Sets default network emulator settings. If network emulator is disabled do nothing.
   * 
   * @param lossPercent loss in percents
   * @param meanDelay mean delay
   */
  public void setDefaultLinkSettings(int lossPercent, int meanDelay) {
    if (!enabled) {
      LOGGER.warn("Can't set default network settings (loss={}%, mean={}ms) for {} since network emulator is disabled",
          lossPercent, meanDelay, address);
      return;
    }
    defaultLinkSettings = new NetworkLinkSettings(lossPercent, meanDelay);
    LOGGER.info("Set default network settings (loss={}%, mean={}ms) for {}", lossPercent, meanDelay, address);
  }

  /**
   * Blocks messages to the given destinations. If network emulator is disabled do nothing.
   * 
   * @param destinations collection of target endpoints where to apply
   */
  public void block(Address... destinations) {
    block(Arrays.asList(destinations));
  }

  /**
   * Blocks messages to the given destinations. If network emulator is disabled do nothing.
   * 
   * @param destinations collection of target endpoints where to apply
   */
  public void block(Collection<Address> destinations) {
    if (!enabled) {
      LOGGER.warn("Can't block network from {} to {} since network emulator is disabled");
      return;
    }
    for (Address destination : destinations) {
      customLinkSettings.put(destination, DEAD_LINK_SETTINGS);
    }
    LOGGER.info("Blocked network from {} to {}", address, destinations);
  }

  /**
   * Unblocks messages to given destinations. If network emulator is disabled do nothing.
   * 
   * @param destinations collection of target endpoints where to apply
   */
  public void unblock(Address... destinations) {
    unblock(Arrays.asList(destinations));
  }

  /**
   * Unblocks messages to given destinations. If network emulator is disabled do nothing.
   * 
   * @param destinations collection of target endpoints where to apply
   */
  public void unblock(Collection<Address> destinations) {
    if (!enabled) {
      LOGGER.warn("Can't unblock network from {} to {} since network emulator is disabled", address, destinations);
      return;
    }
    for (Address destination : destinations) {
      customLinkSettings.remove(destination);
    }
    LOGGER.info("Unblocked network from {} to {}", address, destinations);
  }

  /**
   * Unblock messages to all destinations. If network emulator is disabled do nothing.
   */
  public void unblockAll() {
    if (!enabled) {
      LOGGER.warn("Can't unblock network from {} since network emulator is disabled", address);
      return;
    }
    customLinkSettings.clear();
    LOGGER.info("Unblocked all network from {}", address);
  }

  /**
   * Returns total message sent count computed by network emulator. If network emulator is disabled returns zero.
   * 
   * @return total message sent
   */
  public long totalMessageSentCount() {
    if (!enabled) {
      LOGGER.warn("Can't compute total messages sent from {} since network emulator is disabled", address);
      return 0;
    }
    return totalMessageSentCount.get();
  }

  /**
   * Returns total message lost count computed by network emulator. If network emulator is disabled returns zero.
   * 
   * @return total message lost
   */
  public long totalMessageLostCount() {
    if (!enabled) {
      LOGGER.warn("Can't compute total messages lost from {} since network emulator is disabled", address);
      return 0;
    }
    return totalMessageLostCount.get();
  }

  // For internal use
  void incrementMessageSentCount() {
    if (!enabled) {
      LOGGER.warn("since network emulator is disabled");
      return;
    }
    totalMessageSentCount.incrementAndGet();
  }

  // For internal use
  void incrementMessageLostCount() {
    if (!enabled) {
      LOGGER.warn("since network emulator is disabled");
      return;
    }
    totalMessageLostCount.incrementAndGet();
  }

}
