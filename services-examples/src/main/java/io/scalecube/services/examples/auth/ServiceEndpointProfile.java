package io.scalecube.services.examples.auth;

import java.util.StringJoiner;

public class ServiceEndpointProfile {

  private final String endpoint;
  private final String serviceRole;

  public ServiceEndpointProfile(String endpoint, String serviceRole) {
    this.endpoint = endpoint;
    this.serviceRole = serviceRole;
  }

  public String endpoint() {
    return endpoint;
  }

  public String serviceRole() {
    return serviceRole;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ServiceEndpointProfile.class.getSimpleName() + "[", "]")
        .add("endpoint='" + endpoint + "'")
        .add("serviceRole='" + serviceRole + "'")
        .toString();
  }
}
