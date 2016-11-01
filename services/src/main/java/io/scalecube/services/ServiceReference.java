package io.scalecube.services;

import io.scalecube.transport.Address;

import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.Objects;

import javax.annotation.concurrent.Immutable;

@Immutable
public class ServiceReference {

  private final String memberId;
  private final String serviceName;
  private final Address address;
  private final String[] tags; //TODO: Replace with some immutable collection since array is mutable

  /**
   * constructor of ServiceReference, a reference for local or remote service instance.
   * @param memberId the memberId of the requested service.
   * @param serviceName the fully qualified name of the service.
   * @param address the address of the service.
   * @param tags optional tags of the service.
   */
  public ServiceReference(String memberId, String serviceName, Address address, String[] tags) {
    Preconditions.checkNotNull(memberId);
    Preconditions.checkNotNull(serviceName);
    this.memberId = memberId;
    this.serviceName = serviceName;
    this.address = address;
    this.tags = tags;
  }

  public String[] tags() {
    return tags;
  }

  public Address address() {
    return address;
  }

  public String memberId() {
    return memberId;
  }

  public String serviceName() {
    return serviceName;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    ServiceReference that = (ServiceReference) obj;
    return Objects.equals(memberId, that.memberId) && Objects.equals(serviceName, that.serviceName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(memberId, serviceName);
  }
  
  @Override
  public String toString() {
    return "ServiceReference [memberId=" + memberId
        + ", qualifier=" + serviceName
        + ", address=" + address
        + ", tags=" + Arrays.toString(tags)
        + "]";
  }
}
