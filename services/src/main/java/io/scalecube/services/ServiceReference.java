package io.scalecube.services;

import java.util.Objects;

import javax.annotation.concurrent.Immutable;

import com.google.common.base.Preconditions;

@Immutable
public class ServiceReference {

  private final String memberId;

  private final String serviceName;

  public ServiceReference(String memberId, String serviceName) {
    Preconditions.checkNotNull(memberId);
    Preconditions.checkNotNull(serviceName);
    this.memberId = memberId;
    this.serviceName = serviceName;
  }

  public String memberId() {
    return memberId;
  }

  public String serviceName() {
    return serviceName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
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

  public static ServiceReference create(ServiceDefinition serviceDefinition, String memberId) {
    return new ServiceReference(memberId, serviceDefinition.serviceName());
  }
}
