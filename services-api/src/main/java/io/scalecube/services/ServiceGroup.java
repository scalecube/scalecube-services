package io.scalecube.services;

import java.util.Objects;

public class ServiceGroup {

  private String id;
  private int size;

  /**
   * Constructor for SerDe.
   *
   * @deprecated exposed only for de/serialization purpose.
   */
  public ServiceGroup() {}

  public ServiceGroup(String id, int size) {
    this.id = id;
    this.size = size;
  }

  public String id() {
    return id;
  }

  public int size() {
    return size;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ServiceGroup that = (ServiceGroup) o;
    return size == that.size && id.equals(that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, size);
  }

  @Override
  public String toString() {
    return "ServiceGroup{" + "id='" + id + '\'' + ", size=" + size + '}';
  }
}
