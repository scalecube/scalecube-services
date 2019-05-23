package io.scalecube.services;

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
  public String toString() {
    return "ServiceGroup{" + "id='" + id + '\'' + ", size=" + size + '}';
  }
}
