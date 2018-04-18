package io.scalecube.services;

import java.util.Map;

public class ServiceMethod {

  private final String name;
  private final String contentType;
  private final Map<String, String> tags;

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
