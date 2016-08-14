package io.scalecube.transport;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Utility class which finds local IP address and currently available server ports.
 *
 * @author Anton Kharenko
 * @author Artem Vysochyn
 */
final class Addressing {

  /**
   * The minimum server port number. Set at 1100 to avoid returning privileged port numbers.
   */
  static final int MIN_PORT_NUMBER = 1100;

  /**
   * The maximum server port number. It should only use up to 49151, as from 49152 up to 65535 are reserved for
   * ephemeral ports.
   */
  static final int MAX_PORT_NUMBER = 49151;

  private Addressing() {}

  /**
   * Finds next available port on given local IP address starting from given port and incrementing port for port count
   * iterations.
   *
   * @param inetAddress local IP address on which check for port availability
   * @param fromPort starting port
   * @param portCount number of ports to check
   * @return Next available port
   * @throws NoSuchElementException if port wasn't found in the given interval
   */
  static int getNextAvailablePort(InetAddress inetAddress, int fromPort, int portCount) {
    int toPort = Math.min(MAX_PORT_NUMBER, fromPort + portCount) - 1;
    for (int port = fromPort; port <= toPort; port++) {
      if (isAvailablePort(inetAddress, port)) {
        return port;
      }
    }
    throw new NoSuchElementException("Could not find an available port from " + fromPort + " to " + toPort);
  }

  /**
   * Checks to see if a specific port is available.
   *
   * @param port the port to check for availability
   */
  static boolean isAvailablePort(InetAddress inetAddress, int port) {
    if (port < MIN_PORT_NUMBER || port > MAX_PORT_NUMBER) {
      throw new IllegalArgumentException("Invalid port number: " + port);
    }

    Socket socket = null;
    try {
      socket = new Socket(inetAddress, port);

      // If the code makes it this far without an exception it means
      // something is using the port and has responded.
      return false;
    } catch (IOException ignore) {
      return true;
    } finally {
      if (socket != null) {
        try {
          socket.close();
        } catch (IOException ignore) {
          // Should not be thrown
        }
      }
    }
  }

  /**
   * Returns {@link InetAddress} by given IP address or network interface name.
   *
   * @param listenAddress listen address; if set then {@code listenInterface} must be not set.
   * @param listenInterface network interface name; if set then {@code listenAddress} must be not set.
   * @param preferIPv6 should we prefer IPv6 when choosing IP address; accounted when {@code listenInterface} is set.
   * @return {@link InetAddress} object.
   * @throws IllegalArgumentException if both {@code listenAddress} and {@code listenInterface} were passed.
   * @throws IllegalArgumentException if {@code listenAddress} can't be resolved or if it reprensents wildcard address,
   *         or if it doesn't belong to any active network interface.
   */
  static InetAddress getLocalIpAddress(String listenAddress, String listenInterface, boolean preferIPv6) {
    InetAddress ipAddress;
    if (!Strings.isNullOrEmpty(listenAddress) && !Strings.isNullOrEmpty(listenInterface)) {
      throw new IllegalArgumentException("Not allowed to set both listenAddress and listenInterface, choose one");
    } else if (!Strings.isNullOrEmpty(listenAddress)) {
      try {
        ipAddress = InetAddress.getByName(listenAddress);
      } catch (UnknownHostException e) {
        throw new IllegalArgumentException("Unknown listenAddress: " + listenAddress);
      }
      // account that 0.0.0.0 is not allowed
      if (ipAddress.isAnyLocalAddress()) {
        throw new IllegalArgumentException("listenAddress: " + listenAddress + " cannot be a wildcard address");
      }
      // ensure address is valid
      if (!isValidLocalIpAddress(ipAddress)) {
        throw new IllegalArgumentException(
            "listenAddress: " + listenAddress + " doesn't belong to any active network interface");
      }
    } else if (!Strings.isNullOrEmpty(listenInterface)) {
      ipAddress = getNetworkInterfaceIpAddress(listenInterface, preferIPv6);
    } else {
      // fallback to local ip address
      ipAddress = getLocalIpAddress();
    }
    return ipAddress;
  }

  /**
   * Getting local IP address by the address of local host. <b>NOTE:</b> returned IP address is expected to be a
   * publicly visible IP address.
   *
   * @throws RuntimeException wrapped {@link UnknownHostException} in case when local host name couldn't be resolved
   *         into an address.
   */
  static InetAddress getLocalIpAddress() {
    try {
      return InetAddress.getLocalHost();
    } catch (UnknownHostException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * @return {@link InetAddress} by network interface name and a flag indicating whether returned IP address will be
   *         IPv4 or IPv6.
   */
  static InetAddress getNetworkInterfaceIpAddress(String listenInterface, boolean preferIPv6) {
    try {
      NetworkInterface ni = NetworkInterface.getByName(listenInterface);
      if (ni == null) {
        throw new IllegalArgumentException("Configured network interface: " + listenInterface + " could not be found");
      }
      if (!ni.isUp()) {
        throw new IllegalArgumentException("Configured network interface: " + listenInterface + " is not active");
      }
      Enumeration<InetAddress> addrs = ni.getInetAddresses();
      if (!addrs.hasMoreElements()) {
        throw new IllegalArgumentException(
            "Configured network interface: " + listenInterface + " was found, but had no addresses");
      }
      // try to return the first address of the preferred type, otherwise return the first address
      InetAddress result = null;
      while (addrs.hasMoreElements()) {
        InetAddress addr = addrs.nextElement();
        if (preferIPv6 && addr instanceof Inet6Address) {
          return addr;
        }
        if (!preferIPv6 && addr instanceof Inet4Address) {
          return addr;
        }
        if (result == null) {
          result = addr;
        }
      }
      return result;
    } catch (SocketException e) {
      throw new IllegalArgumentException(
          "Configured network interface: " + listenInterface + " caused an exception", e);
    }
  }

  /**
   * @return boolean indicating whether given address belongs to any active network interface.
   */
  static boolean isValidLocalIpAddress(InetAddress listenAddress) {
    List<NetworkInterface> networkInterfaces;
    try {
      networkInterfaces = Collections.list(NetworkInterface.getNetworkInterfaces());
    } catch (SocketException e) {
      throw new IllegalArgumentException("Can't get list of network interfaces", e);
    }
    // go and check out network interface by IP address
    for (NetworkInterface ni : networkInterfaces) {
      try {
        if (ni.isUp()) {
          Enumeration<InetAddress> addrs = ni.getInetAddresses();
          while (addrs.hasMoreElements()) {
            InetAddress addr = addrs.nextElement();
            if (addr.getHostAddress().equals(listenAddress.getHostAddress())) {
              return true;
            }
          }
        }
      } catch (SocketException e) {
        throw new IllegalArgumentException("Network interface: " + ni + " caused an exception", e);
      }
    }
    // looked at all network interfaces and didn't match IP address
    return false;
  }
}
