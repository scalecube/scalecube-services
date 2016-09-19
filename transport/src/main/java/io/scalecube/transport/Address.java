package io.scalecube.transport;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Strings;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

@Immutable
public final class Address {

  private static final Pattern ADDRESS_FORMAT = Pattern.compile("(?<host>^.*):(?<port>\\d+$)");

  private final String host;
  private final int port;

  private Address(@CheckForNull String host, int port) {
    checkArgument(!Strings.isNullOrEmpty(host));
    this.host = host;
    this.port = port;
  }

  /**
   * Parses given string to create address instance. For localhost variant host may come in: {@code 127.0.0.1},
   * {@code localhost}; when localhost case detected then node's public IP address would be resolved.
   *
   * @param hostAndPort must come in form {@code host:port}
   */
  public static Address from(@CheckForNull String hostAndPort) {
    checkArgument(!Strings.isNullOrEmpty(hostAndPort));

    Matcher matcher = ADDRESS_FORMAT.matcher(hostAndPort);
    if (!matcher.find()) {
      throw new IllegalArgumentException();
    }

    String host = matcher.group(1);
    checkArgument(!Strings.isNullOrEmpty(host));
    String host1 =
        "localhost".equals(host) || "127.0.0.1".equals(host) ? Addressing.getLocalIpAddress().getHostAddress() : host;
    int port = Integer.parseInt(matcher.group(2));
    return new Address(host1, port);
  }

  /**
   * Creates address from host and port.
   */
  public static Address create(String host, int port) {
    return new Address(host, port);
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
