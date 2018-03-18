package io.scalecube.services;

import io.scalecube.transport.Address;

import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import javax.annotation.concurrent.Immutable;

@Immutable
public class ServiceReference {

  private final String memberId;
  private final String serviceName;
  private final Address address;
  private Set<String> methods;


  /**
   * constructor of ServiceReference, a reference for local or remote service instance.
   * 
   * @param memberId the memberId of the requested service.
   * @param serviceName the fully qualified name of the service.
   * @param methods methods collection
   * @param address the address of the service.
   */
  public ServiceReference(String memberId, String serviceName, Set<String> methods, Address address) {
    Preconditions.checkNotNull(memberId);
    Preconditions.checkNotNull(serviceName);
    this.methods = methods;
    this.memberId = memberId;
    this.serviceName = serviceName;
    this.address = address;
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
        + "]";
  }

  public Set<String> methods() {
    return Collections.unmodifiableSet(methods);
  }


}
