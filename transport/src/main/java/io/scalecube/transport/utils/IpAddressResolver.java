package io.scalecube.transport.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;

/**
 * Utility class that defines node's IP address which is different from localhost.
 */
public class IpAddressResolver {

  private static final Logger LOGGER = LoggerFactory.getLogger(IpAddressResolver.class);

  /**
   * Instantiates a new ip address resolver.
   */
  private IpAddressResolver() {
    /* Can't be instantiated */
  }

  /**
   * Resolve ip address.
   *
   * @return the inet address
   * @throws java.net.UnknownHostException the unknown host exception
   */
  public static InetAddress resolveIpAddress() throws UnknownHostException {
    Enumeration<NetworkInterface> netInterfaces = null;
    try {
      netInterfaces = NetworkInterface.getNetworkInterfaces();
    } catch (SocketException e) {
      LOGGER.error("Socket error during resolving IP address", e);
    }

    while (netInterfaces != null && netInterfaces.hasMoreElements()) {
      NetworkInterface ni = netInterfaces.nextElement();
      Enumeration<InetAddress> address = ni.getInetAddresses();
      while (address.hasMoreElements()) {
        InetAddress addr = address.nextElement();
        LOGGER.debug("Found network interface: {}", addr.getHostAddress());
        if (!addr.isLoopbackAddress() && addr.getAddress().length == 4 // for IP4 addresses
        ) {
          return addr;
        }
      }
    }
    return InetAddress.getLocalHost();
  }

}
