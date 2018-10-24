package io.scalecube.services.api;

import java.util.Objects;

public class Address {

  private final String host;
  private final int port;

  private Address(String host, int port) {
    this.host = host;
    this.port = port;
  }

  /**
   * Create address.
   *
   * @param host host
   * @param port port
   * @return address
   */
  public static Address create(String host, int port) {
    return new Address(host, port);
  }

  /**
   * Create address.
   *
   * @param hostAndPort host:port
   * @return address
   */
  public static Address from(String hostAndPort) {
    String[] split = hostAndPort.split(":");
    if (split.length != 2) {
      throw new IllegalArgumentException();
    }
    String host = split[0];
    int port = Integer.parseInt(split[1]);
    return new Address(host, port);
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
}
