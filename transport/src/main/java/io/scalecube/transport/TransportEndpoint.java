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
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

@Immutable
public final class TransportEndpoint {
  /**
   * Regexp pattern for {@code [host:]port:id}.
   */
  private static final Pattern TRASNPORT_ENDPOINT_ADDRESS_FORMAT = Pattern.compile("(^.*(?=:))?:?(\\d+):(.*$)");
  /**
   * Regexp pattern for {@code host:port}.
   */
  private static final Pattern SOCKET_ADDRESS_FORMAT = Pattern.compile("(^.*):(\\d+$)");

  /**
   * Endpoint identifier (or <i>incarnationId</i>). Either being set upfront or obtained at connection' handshake phase.
   */
  private String id;

  /**
   * Socket address of the endpoint. <b>NOTE:</b> this field isn't serializable.
   */
  private transient volatile InetSocketAddress socketAddress;

  /**
   * Host address. <b>NOTE:</b> {@link #socketAddress}'s host address is eq to value of this field.
   */
  private String host;

  /**
   * Port. <b>NOTE:</b> {@link #socketAddress}'s port is eq to value of this field.
   */
  private int port;

  private TransportEndpoint() {}

  private TransportEndpoint(@CheckForNull String id, @CheckForNull InetSocketAddress socketAddress) {
    checkArgument(id != null);
    checkArgument(socketAddress != null);
    this.id = id;
    this.socketAddress = socketAddress;
    this.host = socketAddress.getAddress().getHostAddress();
    this.port = socketAddress.getPort();
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

  /**
   * Creates transport endpoint from socketAddress object and endpointId.
   *
   * @param id given endpoint id (or <i>incarnationId</i>)
   * @param address a socketAddress
   */
  public static TransportEndpoint from(String id, InetSocketAddress address) {
    return new TransportEndpoint(id, address);
  }

  /**
   * @return local socketAddress by given port.
   */
  public static InetSocketAddress localSocketAddress(int port) {
    return new InetSocketAddress(resolveLocalIpAddress(), port);
  }

  /**
   * Parses given string to get socketAddress. For localhost variant host may come in: {@code 127.0.0.1},
   * {@code localhost} or {@code 0.0.0.0}; when localhost case detected then real local ip address would be resolved.
   *
   * @param input in a form {@code host:port}
   */
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

  @Nonnull
  public String id() {
    return id;
  }

  @Nonnull
  public InetSocketAddress socketAddress() {
    return socketAddress != null ? socketAddress : (socketAddress = new InetSocketAddress(host, port));
  }

  @Nonnull
  public String getString() {
    return host + ":" + port + ":" + id;
  }

  private static boolean isLocalhost(String host) {
    return "localhost".equals(host) || "127.0.0.1".equals(host);
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
    return Objects.equals(id, that.id) && Objects.equals(socketAddress(), that.socketAddress());
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, socketAddress());
  }

  @Override
  public String toString() {
    return "TransportEndpoint{" + getString() + "}";
  }
}
