package io.servicefabric.transport;

import static com.google.common.base.Preconditions.checkArgument;

import java.net.URI;
import java.util.Objects;

import javax.annotation.CheckForNull;
import javax.annotation.concurrent.Immutable;

@Immutable
public final class TransportEndpoint {

  private String id;
  private TransportAddress address;

  private TransportEndpoint() {}

  public TransportEndpoint(@CheckForNull String id, @CheckForNull TransportAddress address) {
    checkArgument(id != null);
    checkArgument(address != null);
    this.id = id;
    this.address = address;
  }

  /**
   * Creates transport endpoint from uri string.
   */
  public static TransportEndpoint from(String uri) {
    URI uri1 = URI.create(uri);
    TransportEndpoint target = new TransportEndpoint();
    target.id = uri1.getUserInfo();
    String substring = uri1.getSchemeSpecificPart().substring(uri1.getSchemeSpecificPart().indexOf("@") + 1);
    target.address = TransportAddress.from(uri1.getScheme() + "://" + substring);
    return target;
  }

  public static TransportEndpoint from(String id, TransportAddress address) {
    return new TransportEndpoint(id, address);
  }

  public String id() {
    return id;
  }

  public TransportAddress address() {
    return address;
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
    return Objects.equals(id, that.id)
        && Objects.equals(address, that.address);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, address);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(address.scheme());
    sb.append("://").append(id);
    sb.append("@");
    String hostAddress = address.hostAddress();
    int port = address.port();
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
