package io.scalecube.transport.utils;

import com.google.common.collect.Ordering;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;

/**
 * Utility class that defines node's IP4 address which is different from localhost. <b>NOTE:</b> first found NIC with
 * IP4 address would be considered.
 */
public class IpAddressResolver {
  private static final Logger LOGGER = LoggerFactory.getLogger(IpAddressResolver.class);

  private static final Comparator<NetworkInterface> NETWORK_INTERFACE_COMPARATOR = new Comparator<NetworkInterface>() {
    @Override
    public int compare(NetworkInterface o1, NetworkInterface o2) {
      return Integer.compare(o1.getIndex(), o2.getIndex());
    }
  };

  private static volatile InetAddress ipAddress;

  private IpAddressResolver() {}

  /**
   * Resolve ip address. Iterates on NICs and takes a first found IP4 address.
   *
   * @return the IP4 address
   * @throws UnknownHostException the unknown host exception
   */
  public static InetAddress resolveIpAddress() throws UnknownHostException {
    if (ipAddress != null) {
      return ipAddress;
    }

    List<NetworkInterface> nics = new ArrayList<>();
    try {
      Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
      while (networkInterfaces.hasMoreElements()) {
        nics.add(networkInterfaces.nextElement());
      }
    } catch (SocketException e) {
      LOGGER.error("Socket error during resolving IP address", e);
    }

    for (NetworkInterface nic : Ordering.from(NETWORK_INTERFACE_COMPARATOR).immutableSortedCopy(nics)) {
      Enumeration<InetAddress> address = nic.getInetAddresses();
      while (address.hasMoreElements()) {
        InetAddress addr = address.nextElement();
        if (!addr.isLoopbackAddress() && addr.getAddress().length == 4) {
          return ipAddress = addr; // for IP4 addresses
        }
      }
    }

    return InetAddress.getLocalHost();
  }
}
