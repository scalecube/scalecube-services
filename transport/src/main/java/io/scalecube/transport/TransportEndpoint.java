package io.scalecube.transport;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.transport.utils.IpAddressResolver;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.CheckForNull;
import javax.annotation.concurrent.Immutable;

@Immutable
public final class TransportEndpoint {
  /** Regexp pattern for {@code [host:]port:id} */
  private static final Pattern TRASNPORT_ENDPOINT_ADDRESS_FORMAT = Pattern.compile("(^.*(?=:))?:?(\\d+):(.*$)");
  /** Regexp pattern for {@code host:port} */
  private static final Pattern SOCKET_ADDRESS_FORMAT = Pattern.compile("(^.*):(\\d+$)");

  private static volatile InetSocketAddress localSocketAddress;

  /** Endpoint identifier (or <i>incarnation id</i>). */
  private String id;
  /** Socket address of the endpoint. */
  private InetSocketAddress socketAddress;

  private TransportEndpoint() {}

  public TransportEndpoint(@CheckForNull String id, @CheckForNull InetSocketAddress socketAddress) {
    checkArgument(id != null);
    checkArgument(socketAddress != null);
    this.id = id;
    this.socketAddress = socketAddress;
  }

  /**
   * Creates transport endpoint from uri string. For localhost variant host may come in: {@code 127.0.0.1},
   * {@code localhost}, {@code 0.0.0.0} or omitted et al; when localhost case detected then real local ip address would
   * be resolved.
   *
   * @param input must come in form {@code [host:]port:id}
   */
  public static TransportEndpoint from(@CheckForNull String input) {
    checkArgument(input != null);
    checkArgument(!input.isEmpty());

    Matcher matcher = TRASNPORT_ENDPOINT_ADDRESS_FORMAT.matcher(input);
    if (!matcher.find()) {
      throw new IllegalArgumentException();
    }

    String host = Optional.fromNullable(matcher.group(1)).or(resolveLocalIpAddress());
    if (isLocalhost(host)) {
      host = resolveLocalIpAddress();
    }

    int port = Integer.parseInt(matcher.group(2));
    String id = matcher.group(3);

    return new TransportEndpoint(id, new InetSocketAddress(host, port));
  }

  public static TransportEndpoint from(String id, InetSocketAddress address) {
    return new TransportEndpoint(id, address);
  }

  public static InetSocketAddress localSocketAddress(int port) {
    return localSocketAddress != null ? localSocketAddress
        : (localSocketAddress = new InetSocketAddress(resolveLocalIpAddress(), port));
  }

  public static InetSocketAddress parseSocketAddress(@CheckForNull String input) {
    checkArgument(input != null);
    checkArgument(!input.isEmpty());

    Matcher matcher = SOCKET_ADDRESS_FORMAT.matcher(input);
    if (!matcher.find()) {
      throw new IllegalArgumentException();
    }

    String host = Optional.fromNullable(matcher.group(1)).or(resolveLocalIpAddress());
    if (isLocalhost(host)) {
      host = resolveLocalIpAddress();
    }

    int port = Integer.parseInt(matcher.group(2));

    return new InetSocketAddress(host, port);
  }

  public String getId() {
    return id;
  }

  public InetSocketAddress getSocketAddress() {
    return socketAddress;
  }

  private static boolean isLocalhost(String host) {
    return "localhost".equals(host) || "127.0.0.1".equals(host) || "0.0.0.0".equals(host);
  }

  private static String resolveLocalIpAddress() {
    try {
      return IpAddressResolver.resolveIpAddress().getHostAddress();
    } catch (UnknownHostException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    TransportEndpoint that = (TransportEndpoint) other;
    return Objects.equals(id, that.id) && Objects.equals(socketAddress, that.socketAddress);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, socketAddress);
  }

  @Override
  public String toString() {
    return "TransportEndpoint{" +
        socketAddress.getAddress().getHostAddress() + ":" + socketAddress.getPort() + ":" + id
        + "}";
  }
}
