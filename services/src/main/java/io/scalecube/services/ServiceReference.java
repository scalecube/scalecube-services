package io.scalecube.services;

import io.scalecube.transport.Address;

import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.Objects;

import javax.annotation.concurrent.Immutable;

@Immutable
public class ServiceReference {

  private final String memberId;

  private final String qualifier;

  private final Address address;

  private String[] tags;

  /**
   * constructor of ServiceReference, a reference for local or remote service instance.
   * @param memberId the memberId of the requested service.
   * @param qualifier the fully qualified name of the service.
   * @param address the address of the service.
   * @param tags optional tags of the service.
   */
  public ServiceReference(String memberId, String qualifier, Address address, String[] tags) {
    Preconditions.checkNotNull(memberId);
    Preconditions.checkNotNull(qualifier);
    this.memberId = memberId;
    this.qualifier = qualifier;
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

  public String qualifier() {
    return qualifier;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    ServiceReference that = (ServiceReference) obj;
    return Objects.equals(memberId, that.memberId) && Objects.equals(qualifier, that.qualifier);
  }

  @Override
  public int hashCode() {
    return Objects.hash(memberId, qualifier);
  }

  public static ServiceReference create(ServiceDefinition serviceDefinition, String memberId, Address address,
      String[] tags) {
    return new ServiceReference(memberId, serviceDefinition.qualifier(), address, tags);
  }
  
  @Override
  public String toString() {
    return "ServiceReference [memberId=" + memberId + ", qualifier=" + qualifier + ", address=" + address + ", tags="
        + Arrays.toString(tags) + "]";
  }
}
