package io.scalecube.transport;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.transport.utils.IpAddressResolver;

import com.google.common.base.Throwables;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Objects;

import javax.annotation.CheckForNull;
import javax.annotation.concurrent.Immutable;

@Immutable
public final class TransportEndpoint {

  private String id;
  private InetSocketAddress socketAddress;

  private TransportEndpoint() {}

  public TransportEndpoint(@CheckForNull String id, @CheckForNull InetSocketAddress socketAddress) {
    checkArgument(id != null);
    checkArgument(socketAddress != null);
    this.id = id;
    this.socketAddress = socketAddress;
  }

  /**
   * Creates transport endpoint from uri string.
   *
   * @param uri must come in form {@code tcp://id@[host:]port}
   */
  public static TransportEndpoint from(String uri) {
    URI uri1 = URI.create(uri);
    if (uri1.getUserInfo() == null || uri1.getUserInfo().isEmpty()) {
      throw new IllegalArgumentException(uri);
    }

    TransportEndpoint target = new TransportEndpoint();
    target.id = uri1.getUserInfo(); // set transport endpoint id

    String substring = uri1.getSchemeSpecificPart().substring(uri1.getSchemeSpecificPart().indexOf("@") + 1);
    if (substring.isEmpty()) {
      throw new IllegalArgumentException(uri);
    }

    // assume given local getSocketAddress, w/o host; like tcp://id@port
    try {
      int port = Integer.valueOf(substring);
      target.socketAddress = new InetSocketAddress(port);
      return target;
    } catch (NumberFormatException ignore) {
    }

    // check that given host specific (non-local) getSocketAddress; like tcp://id@host:port
    String[] hostAndPort = substring.split(":");
    if (hostAndPort.length != 2) {
      throw new IllegalArgumentException(uri);
    }

    int port;
    try {
      port = Integer.valueOf(hostAndPort[1].trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(uri);
    }
    String address = hostAndPort[0].trim();
    String hostAddress = isLocalhost(address) ? resolveIpAddress() : address;

    target.socketAddress = new InetSocketAddress(hostAddress, port);
    return target;
  }

  public static TransportEndpoint from(String id, InetSocketAddress address) {
    return new TransportEndpoint(id, address);
  }

  public String id() {
    return id;
  }

  public InetSocketAddress getSocketAddress() {
    return socketAddress;
  }

  private static boolean isLocalhost(String host) {
    return "localhost".equals(host) || "127.0.0.1".equals(host);
  }

  private static String resolveIpAddress() {
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
    return id + "@" + socketAddress.getAddress().getHostAddress() + ":" + socketAddress.getPort();
  }
}
