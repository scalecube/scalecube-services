package io.servicefabric.services.registry;

import io.protostuff.Tag;

import java.util.Objects;

public class ServiceReference {

  @Tag(1)
  private final String name;

  @Tag(2)
  private final String memberId;

  public ServiceReference(String name, String memberId) {
    this.name = name;
    this.memberId = memberId;
  }

  public String name() {
    return name;
  }

  public String memberId() {
    return memberId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ServiceReference that = (ServiceReference) o;
    return Objects.equals(name, that.name) &&
        Objects.equals(memberId, that.memberId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, memberId);
  }

  @Override
  public String toString() {
    return name + ':' + memberId;
  }
}
