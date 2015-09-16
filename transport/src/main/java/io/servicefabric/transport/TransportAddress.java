package io.servicefabric.transport;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.propagate;

import io.servicefabric.transport.utils.IpAddressResolver;

import com.google.common.base.Splitter;

import java.net.URI;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Objects;

import javax.annotation.concurrent.Immutable;

@Immutable
public final class TransportAddress {

  public static final String TCP_SCHEME = "tcp";

  private final String scheme;
  private final String hostAddress;
  private final int port;

  private TransportAddress(String scheme, String hostAddress, int port) {
    checkArgument(scheme != null);
    checkArgument(hostAddress != null);
    checkArgument(port > 0);
    this.scheme = scheme;
    this.hostAddress = hostAddress;
    this.port = port;
  }

  /**
   * @param port port
   * @return new local (in terms of tcp) TransportEndpoint object; resolves real ip.
   * @throws IllegalArgumentException if {@code basePort} is invalid / or {@code portShift} is invalid
   */
  public static TransportAddress localTcp(int port) throws IllegalArgumentException {
    return new TransportAddress(TCP_SCHEME, resolveIp(), port);
  }

  /**
   * Creates new transport endpoint from uri.
   * 
   * @param uri must come in form {@code tcp}://[host:]port}
   * @return TransportEndpoint object
   * @throws IllegalArgumentException if scheme is wrong / or port is invalid
   */
  public static TransportAddress from(String uri) throws IllegalArgumentException {
    URI uri1 = URI.create(uri);
    String scheme = uri1.getScheme();
    int port;
    String hostAddress;
    switch (scheme) {
      case TCP_SCHEME:
        port = uri1.getPort();
        if (port == -1) {
          try {
            port = Integer.valueOf(uri1.getAuthority());
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException("NFE invalid port, uri=" + uri);
          }
          hostAddress = resolveIp();
        } else {
          String host = uri1.getHost();
          hostAddress = isLocal(host) ? resolveIp() : uri1.getHost();
        }
        break;
      default:
        throw new IllegalArgumentException(uri);
    }
    return new TransportAddress(scheme, hostAddress, port);
  }

  public static TransportAddress tcp(String hostnameAndPort) {
    List<String> addressAndPort = Splitter.on(':').splitToList(hostnameAndPort);
    int port;
    String hostAddress;
    if (addressAndPort.size() != 2) {
      throw new IllegalArgumentException("hostname:port expected, token=" + hostnameAndPort);
    }
    try {
      port = Integer.valueOf(addressAndPort.get(1).trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("NFE invalid port, token=" + hostnameAndPort);
    }
    String address = addressAndPort.get(0).trim();
    hostAddress = isLocal(address) ? resolveIp() : address;
    return new TransportAddress(TCP_SCHEME, hostAddress, port);
  }

  private static boolean isLocal(String host) {
    return "localhost".equals(host) || "127.0.0.1".equals(host);
  }

  private static String resolveIp() {
    try {
      return IpAddressResolver.resolveIpAddress().getHostAddress();
    } catch (UnknownHostException e) {
      throw propagate(e);
    }
  }

  public String scheme() {
    return scheme;
  }

  public String hostAddress() {
    return hostAddress;
  }

  public int port() {
    return port;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    TransportAddress that = (TransportAddress) o;
    return Objects.equals(port, that.port) && Objects.equals(scheme, that.scheme)
        && Objects.equals(hostAddress, that.hostAddress);
  }

  @Override
  public int hashCode() {
    return Objects.hash(scheme, hostAddress, port);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(scheme);
    sb.append("://");
    if (hostAddress != null) {
      sb.append(hostAddress);
    }
    if (port > 0) {
      if (hostAddress != null) {
        sb.append(":");
      }
      sb.append(port);
    }
    return sb.toString();
  }
}
