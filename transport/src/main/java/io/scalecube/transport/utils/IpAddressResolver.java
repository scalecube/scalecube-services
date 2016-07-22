package io.scalecube.transport.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;

/**
 * Utility class that defines node's IP4 socketAddress which is different from localhost. <br/>
 * <b>NOTE:<b/> first found NIC with IP4 address would be considered.
 */
public class IpAddressResolver {
  private static final Logger LOGGER = LoggerFactory.getLogger(IpAddressResolver.class);

  private static volatile InetAddress ipAddress;

  private IpAddressResolver() {}

  /**
   * Resolve ip socketAddress. Iterates on NICs and takes a first found IP4 address.
   *
   * @return the IP4 address
   * @throws UnknownHostException the unknown host exception
   */
  public static InetAddress resolveIpAddress() throws UnknownHostException {
    if (ipAddress != null) {
      return ipAddress;
    }

    Enumeration<NetworkInterface> netInterfaces = null;
    try {
      netInterfaces = NetworkInterface.getNetworkInterfaces();
    } catch (SocketException e) {
      LOGGER.error("Socket error during resolving IP socketAddress", e);
    }
    while (netInterfaces != null && netInterfaces.hasMoreElements()) {
      NetworkInterface ni = netInterfaces.nextElement();
      Enumeration<InetAddress> address = ni.getInetAddresses();
      while (address.hasMoreElements()) {
        InetAddress addr = address.nextElement();
        if (!addr.isLoopbackAddress() && addr.getAddress().length == 4) {
          return ipAddress = addr; // for IP4 addresses
        }
      }
    }

    throw new UnsupportedOperationException();
  }
}
