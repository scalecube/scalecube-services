package io.scalecube.services;

import java.util.Objects;

import javax.annotation.concurrent.Immutable;

import com.google.common.base.Preconditions;

import io.scalecube.transport.Address;

@Immutable
public class ServiceReference {

  private final String memberId;

  private final String qualifier;

  private final Address address;

  private String[] tags;

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
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    ServiceReference that = (ServiceReference) o;
    return Objects.equals(memberId, that.memberId) &&
        Objects.equals(qualifier, that.qualifier);
  }

  @Override
  public int hashCode() {
    return Objects.hash(memberId, qualifier);
  }

  @Override
  public String toString() {
    return "ServiceReference{" +
        "memberId='" + memberId + '\'' +
        ", qualifier='" + qualifier + '\'' +
        '}';
  }

  public static ServiceReference create(ServiceDefinition serviceDefinition, String memberId, Address address,
      String[] tags) {
    return new ServiceReference(memberId, serviceDefinition.qualifier(), address, tags);
  }
}
