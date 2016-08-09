package io.scalecube.transport;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.NoSuchElementException;

/**
 * Utility class which finds currently available server ports.
 *
 * @author Anton Kharenko
 */
final class AvailablePortFinder {

  /**
   * The minimum server port number. Set at 1100 to avoid returning privileged port numbers.
   */
  public static final int MIN_PORT_NUMBER = 1100;

  /**
   * The maximum server port number. It should only use up to 49151, as from 49152 up to 65535
   * are reserved for ephemeral ports.
   */
  public static final int MAX_PORT_NUMBER = 49151;

  /**
   * Finds next available port starting from given port and incrementing port for port count iterations.
   *
   * @param fromPort starting port
   * @param portCount number of ports to check
   * @return Next available port
   * @throws NoSuchElementException if port wasn't found in the given interval
   */
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

    Socket socket = null;
    try {
      socket = new Socket(Address.getLocalIpAddress(), port);

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

}
