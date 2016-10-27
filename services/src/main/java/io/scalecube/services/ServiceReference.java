package io.scalecube.services;

import java.lang.reflect.Type;
import java.util.Objects;

import javax.annotation.concurrent.Immutable;

import com.google.common.base.Preconditions;

import io.scalecube.transport.Address;

@Immutable
public class ServiceReference {

  private final String memberId;

  private final String serviceName;

  private final Address address;

  private String[] tags;

  private final Type returnType;



  public ServiceReference(String memberId, String serviceName, Address address, String[] tags,Type returnType) {
    Preconditions.checkNotNull(memberId);
    Preconditions.checkNotNull(serviceName);
    this.memberId = memberId;
    this.serviceName = serviceName;
    this.address = address;
    this.tags = tags;
    this.returnType = returnType;
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
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    ServiceReference that = (ServiceReference) o;
    return Objects.equals(memberId, that.memberId) &&
        Objects.equals(serviceName, that.serviceName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(memberId, serviceName);
  }

  @Override
  public String toString() {
    return "ServiceReference{" +
        "memberId='" + memberId + '\'' +
        ", serviceName='" + serviceName + '\'' +
        '}';
  }

  public static ServiceReference create(ServiceDefinition serviceDefinition, String memberId, Address address,
      String[] tags) {
    return new ServiceReference(memberId, serviceDefinition.qualifier(), address, tags,serviceDefinition.returnType());
  }

  public Type returnType() {
    return returnType;
  }
}
