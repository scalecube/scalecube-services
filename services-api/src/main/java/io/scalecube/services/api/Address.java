package io.scalecube.services.api;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import reactor.core.Exceptions;

public class Address {

  private static final Pattern ADDRESS_FORMAT = Pattern.compile("(?<host>^.*):(?<port>\\d+$)");

  private final String host;
  private final int port;

  public static Address create(String host, int port) {
    return new Address(host, port);
  }

  /**
   * Parses given string to create address instance. For localhost variant host may come in: {@code
   * 127.0.0.1}, {@code localhost}; when localhost case detected then node's public IP address would
   * be resolved.
   *
   * @param hostAndPort must come in form {@code host:port}
   */
  public static Address from(String hostAndPort) {
    requireNonEmpty(hostAndPort);

    Matcher matcher = ADDRESS_FORMAT.matcher(hostAndPort);
    if (!matcher.find()) {
      throw new IllegalArgumentException();
    }

    String host = matcher.group(1);
    requireNonEmpty(host);

    String host1 =
        "localhost".equals(host) || "127.0.0.1".equals(host)
            ? getLocalIpAddress().getHostAddress()
            : host;
    int port = Integer.parseInt(matcher.group(2));
    return new Address(host1, port);
  }

  private Address(String host, int port) {
    requireNonEmpty(host);
    this.host = host;
    this.port = port;
  }

  public String host() {
    return this.host;
  }

  public int port() {
    return this.port;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Address address = (Address) o;
    return port == address.port && Objects.equals(host, address.host);
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, port);
  }

  @Override
  public String toString() {
    return this.host + ":" + this.port;
  }

  private static void requireNonEmpty(String string) {
    if (string == null || string.length() == 0) {
      throw new IllegalArgumentException();
    }
  }

  /**
   * Getting local IP address by the address of local host. <b>NOTE:</b> returned IP address is
   * expected to be a publicly visible IP address.
   *
   * @throws RuntimeException wrapped {@link UnknownHostException} in case when local host name
   *     couldn't be resolved into an address.
   */
  private static InetAddress getLocalIpAddress() {
    try {
      return InetAddress.getLocalHost();
    } catch (UnknownHostException e) {
      throw Exceptions.propagate(e);
    }
  }
}
