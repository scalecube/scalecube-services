package io.scalecube.services;

import java.util.Map;

public class ServiceMethod {

  private String action;
  private String contentType;
  private Map<String, String> tags;

  /**
   * @deprecated exposed only for deserialization purpose.
   */
  public ServiceMethod() {}

  public ServiceMethod(String methodName,
      String contentType,
      Map<String, String> tags) {
    this.action = methodName;
    this.contentType = contentType;
    this.tags = tags;
  }

  public String action() {
    return action;
  }

  public String contentType() {
    return contentType;
  }

  public Map<String, String> tags() {
    return tags;
  }
}
