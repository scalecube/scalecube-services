package io.scalecube.transport;

import java.util.Objects;

public final class TransportChannel {

  private Address address;
  private String name;

  TransportChannel(String name, Address address) {
    this.address = address;
    this.name = name;
  }

  public static TransportChannel create(String name, Address address) {
    return new TransportChannel(Objects.requireNonNull(name), Objects.requireNonNull(address, "address is null"));
  }

  public Address address() {
    return this.address;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return Objects.hash(address, name);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof TransportChannel))
      return false;
    TransportChannel other = (TransportChannel) obj;
    if (!name.equals(other.name))
      return false;
    if (!address.equals(other.address))
      return false;

    return true;
  }

}
