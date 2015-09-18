package io.servicefabric.services.registry;

import io.protostuff.Tag;

import java.util.Objects;

public class ServiceReference {

  @Tag(1)
  private final String name;

  @Tag(2)
  private final String id; // TODO: member id

  public ServiceReference(String name, String id) {
    this.name = name;
    this.id = id;
  }

  public String name() {
    return name;
  }

  public String id() {
    return id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ServiceReference that = (ServiceReference) o;
    return Objects.equals(name, that.name) &&
        Objects.equals(id, that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, id);
  }

  @Override
  public String toString() {
    return name + ':' + id;
  }
}
