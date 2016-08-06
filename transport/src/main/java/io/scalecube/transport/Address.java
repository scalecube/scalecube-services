package io.scalecube.transport;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

@Immutable
public final class Address {

  private static final Logger LOGGER = LoggerFactory.getLogger(Address.class);

  private static final Pattern ADDRESS_FORMAT = Pattern.compile("(?<host>^.*):(?<port>\\d+$)");

  private String host;
  private int port;

  private Address() {}

  private Address(@CheckForNull String host, int port) {
    checkArgument(!Strings.isNullOrEmpty(host));
    this.host = host;
    this.port = port;
  }

  /**
   * Parses given string to create address instance. For localhost variant host may come in: {@code 127.0.0.1},
   * {@code localhost}; when localhost case detected then node's public IP address would be resolved.
   *
   * @param input must come in form {@code host:port}
   */
  public static Address from(@CheckForNull String input) {
    checkArgument(!Strings.isNullOrEmpty(input));

    Matcher matcher = ADDRESS_FORMAT.matcher(input);
    if (!matcher.find()) {
      throw new IllegalArgumentException();
    }

    String host = resolveHost(matcher.group(1));
    int port = Integer.parseInt(matcher.group(2));
    return new Address(host, port);
  }

  /**
   * Creates address from host and port.
   */
  public static Address create(String host, int port) {
    return new Address(host, port);
  }

  /**
   * Creates local address from port.
   * <b>NOTE:</b> hostname of created transport will be set to node's public IP address.
   *
   * @param port a port to bind to.
   */
  public static Address createLocal(int port) {
    return new Address(getLocalIpAddress(), port);
  }

  private static String resolveHost(@CheckForNull String host) {
    checkArgument(!Strings.isNullOrEmpty(host));
    return "localhost".equals(host) || "127.0.0.1".equals(host) ? getLocalIpAddress() : host;
  }

  /**
   * Getting local IP address by the address of local host. <b>NOTE:</b> returned IP address is expected to be a
   * publicly visible IP address.
   *
   * @throws RuntimeException wrapped {@link UnknownHostException} in case when local host name couldn't be resolved
   *         into an address.
   */
  public static String getLocalIpAddress() {
    try {
      return InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      LOGGER.error("Unable to determine local hostname, cause: {}", new Object[] {e});
      throw Throwables.propagate(e);
    }
  }

  /**
   * Host address.
   */
  @Nonnull
  public String host() {
    return host;
  }

  /**
   * Port.
   */
  public int port() {
    return port;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    Address that = (Address) other;
    return Objects.equals(host, that.host) && Objects.equals(port, that.port);
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, port);
  }

  @Override
  public String toString() {
    return host + ":" + port;
  }
}
