package io.scalecube.transport;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Throwables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
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
  private static final Logger LOGGER = LoggerFactory.getLogger(TransportEndpoint.class);

  private static final Pattern TRASNPORT_ENDPOINT_ADDRESS_FORMAT =
      Pattern.compile("(?<host>^.*?):(?<port>\\d+):(?<id>.*$)");

  private static final Pattern SOCKET_ADDRESS_FORMAT =
      Pattern.compile("(?<host>^.*):(?<port>\\d+$)");

  /**
   * Endpoint identifier (or <i>incarnationId</i>). Either being set upfront or obtained at connection' handshake phase.
   */
  private String id;

  /**
   * Socket address of the endpoint. A call {@code socketAddress.isUnresolved()} would return {@code true}, i.e. only
   * {@link InetSocketAddress#getHostName()}, {@link InetSocketAddress#getPort()} will be accessible. <b>NOTE:</b> this
   * field isn't serializable.
   */
  private transient volatile InetSocketAddress socketAddress;

  /**
   * Host address. <b>NOTE:</b> {@link #socketAddress}'s hostname ({@link InetSocketAddress#getHostName()}) is eq to
   * value of this field.
   */
  private String host;

  /**
   * Port. <b>NOTE:</b> {@link #socketAddress}'s port ({@link InetSocketAddress#getPort()}) is eq to value of this
   * field.
   */
  private int port;

  private TransportEndpoint() {}

  private TransportEndpoint(@CheckForNull String id, @CheckForNull InetSocketAddress socketAddress) {
    checkArgument(id != null);
    checkArgument(!id.isEmpty());
    checkArgument(socketAddress != null);
    this.id = id;
    this.socketAddress = socketAddress;
    this.host = socketAddress.getHostName();
    this.port = socketAddress.getPort();
  }

  /**
   * Creates transport endpoint from uri string. For localhost variant host may come in: {@code 127.0.0.1},
   * {@code localhost} or omitted et al; when localhost case detected then node's public IP address would be resolved.
   *
   * @param input must come in form {@code host:port:id}
   */
  public static TransportEndpoint from(@CheckForNull String input) {
    checkArgument(input != null);
    checkArgument(!input.isEmpty());

    Matcher matcher = TRASNPORT_ENDPOINT_ADDRESS_FORMAT.matcher(input);
    if (!matcher.find()) {
      throw new IllegalArgumentException();
    }

    String host = parseHost(matcher.group(1));
    int port = Integer.parseInt(matcher.group(2));
    String id = matcher.group(3);
    return new TransportEndpoint(id, InetSocketAddress.createUnresolved(host, port));
  }

  /**
   * Creates local transport endpoint from endpoint id and port. <b>NOTE:</b> hostname of created transport will be set
   * to node's public IP address.
   *
   * @param id endpoint id (or <i>incarnationId</i>)
   * @param port a port to bind to.
   */
  public static TransportEndpoint createLocal(String id, int port) {
    return new TransportEndpoint(id, InetSocketAddress.createUnresolved(getLocalIpAddress(), port));
  }

  /**
   * Parses given string to get socketAddress (<b>NOTE:</b> not attemp will be made to resolve {@link InetAddress})
   * object. For localhost variant host may come in: {@code 127.0.0.1} or {@code localhost}; when localhost case
   * detected then node's public IP address would be resolved.
   *
   * @param input in a form {@code host:port}
   * @return unresolved socketAddress; only {@link InetSocketAddress#getHostName()}, {@link InetSocketAddress#getPort()}
   *         will be accessible.
   */
  public static InetSocketAddress parseSocketAddress(@CheckForNull String input) {
    checkArgument(input != null);
    checkArgument(!input.isEmpty());

    Matcher matcher = SOCKET_ADDRESS_FORMAT.matcher(input);
    if (!matcher.find()) {
      throw new IllegalArgumentException();
    }

    String host = parseHost(matcher.group(1));
    int port = Integer.parseInt(matcher.group(2));
    return InetSocketAddress.createUnresolved(host, port);
  }

  private static String parseHost(@CheckForNull String host) {
    checkArgument(host != null);
    checkArgument(!host.isEmpty());
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
   * See {@link #host}.
   */
  @Nonnull
  public String host() {
    return host;
  }

  /**
   * See {@link #port}.
   */
  public int port() {
    return port;
  }

  /**
   * See {@link #id}.
   */
  @Nonnull
  public String id() {
    return id;
  }

  /**
   * See {@link #socketAddress}.
   */
  @Nonnull
  public InetSocketAddress socketAddress() {
    return socketAddress != null ? socketAddress : (socketAddress = InetSocketAddress.createUnresolved(host, port));
  }

  @Nonnull
  public String getString() {
    return host + ":" + port + ":" + id;
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
    return Objects.equals(id, that.id) && Objects.equals(host, that.host) && Objects.equals(port, that.port);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, host, port);
  }

  @Override
  public String toString() {
    return "TransportEndpoint{" + getString() + "}";
  }
}
