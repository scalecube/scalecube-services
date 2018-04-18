package io.scalecube.services;

import java.util.Map;

public class ServiceMethod {

  private String name;
  private String contentType;
  private Map<String, String> tags;

  /**
   * @deprecated exposed only for deserialization purpose.
   */
  public ServiceMethod() {}

  public ServiceMethod(String name,
      String contentType,
      Map<String, String> tags) {
    this.name = name;
    this.contentType = contentType;
    this.tags = tags;
  }

  public String name() {
    return name;
  }

  public String contentType() {
    return contentType;
  }

  public Map<String, String> tags() {
    return tags;
  }
}
