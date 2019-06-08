package io.scalecube.services.discovery;

import io.scalecube.services.transport.api.Address;
import java.util.Arrays;

public class ClusterAddresses {

  private ClusterAddresses() {
    // Do not instantiate
  }

  /**
   * Converts one address to another.
   *
   * @param address scalecube services address
   * @return scalecube cluster address
   */
  public static io.scalecube.transport.Address toAddress(
      io.scalecube.services.transport.api.Address address) {
    return io.scalecube.transport.Address.create(address.host(), address.port());
  }

  /**
   * Converts one address array to another address array.
   *
   * @param addresses scalecube services addresses
   * @return scalecube cluster addresses
   */
  public static io.scalecube.transport.Address[] toAddresses(Address[] addresses) {
    return Arrays.stream(addresses)
        .map(ClusterAddresses::toAddress)
        .toArray(io.scalecube.transport.Address[]::new);
  }
}
