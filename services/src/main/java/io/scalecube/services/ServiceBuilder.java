package io.scalecube.services;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class ServiceBuilder {

  private Map<String, String> tags;
  private Set<String> methods;
  private String serviceName; 
  
  public Stream<ServiceBuilder> stream() {
    return null;
  }

  public Set<String> methods() {
    return methods;
  }

  public Map<String, String> tags() {
    return tags;
  }

  public String serviceName() {
    return serviceName;
  }
}
