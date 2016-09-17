package io.scalecube.services;

import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import javax.annotation.concurrent.Immutable;

/**
 * @author Anton Kharenko
 */
@Immutable
public class ServiceReference {

  private final String memberId;

  private final String serviceName;

  private final Set<String> methodNames;

  public ServiceReference(String memberId, String serviceName, Set<String> methodNames) {
    Preconditions.checkNotNull(memberId);
    Preconditions.checkNotNull(serviceName);
    Preconditions.checkNotNull(methodNames);
    this.memberId = memberId;
    this.serviceName = serviceName;
    this.methodNames = new HashSet<>(methodNames);
  }

  public String memberId() {
    return memberId;
  }

  public String serviceName() {
    return serviceName;
  }

  public Set<String> methodNames() {
    return Collections.unmodifiableSet(methodNames);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ServiceReference that = (ServiceReference) o;
    return Objects.equals(memberId, that.memberId) &&
        Objects.equals(serviceName, that.serviceName) &&
        Objects.equals(methodNames, that.methodNames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(memberId, serviceName, methodNames);
  }

  @Override
  public String toString() {
    return "ServiceReference{" +
        "memberId='" + memberId + '\'' +
        ", serviceName='" + serviceName + '\'' +
        ", methodNames=" + methodNames +
        '}';
  }
}
