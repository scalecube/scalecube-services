package io.scalecube.transport.utils;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.util.NoSuchElementException;

/**
 * Utility class which finds currently available server ports.
 *
 * @author Anton Kharenko
 */
public class AvailablePortFinder {

  /**
   * The minimum server port number. Set at 1100 to avoid returning privileged port numbers.
   */
  public static final int MIN_PORT_NUMBER = 1100;

  /**
   * The maximum server port number. It should only use up to 49151, as from 49152 up to 65535
   * are reserved for ephemeral ports.
   */
  public static final int MAX_PORT_NUMBER = 49151;

  public static int getNextAvailable(int fromPort, int portCount) {
    int toPort = Math.min(MAX_PORT_NUMBER, fromPort + portCount) - 1;
    for (int port = fromPort; port <= toPort; port++) {
      if (available(port)) {
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
  public static boolean available(int port) {
    if (port < MIN_PORT_NUMBER || port > MAX_PORT_NUMBER) {
      throw new IllegalArgumentException("Invalid port number: " + port);
    }

    ServerSocket ss = null;
    DatagramSocket ds = null;
    try {
      ss = new ServerSocket(port);
      ss.setReuseAddress(true);
      ds = new DatagramSocket(port);
      ds.setReuseAddress(true);
      return true;
    } catch (IOException ignore) {
      // Do nothing
    } finally {
      if (ds != null) {
        ds.close();
      }

      if (ss != null) {
        try {
          ss.close();
        } catch (IOException ignore) {
          // Should not be thrown
        }
      }
    }
    return false;
  }

}
