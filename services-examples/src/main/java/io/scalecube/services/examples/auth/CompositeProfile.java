package io.scalecube.services.examples.auth;

import java.util.StringJoiner;

public class CompositeProfile {

  private final ServiceEndpointProfile serviceEndpointProfile;
  private final UserProfile userProfile;

  public CompositeProfile(ServiceEndpointProfile serviceEndpointProfile, UserProfile userProfile) {
    this.serviceEndpointProfile = serviceEndpointProfile;
    this.userProfile = userProfile;
  }

  public ServiceEndpointProfile serviceEndpointProfile() {
    return serviceEndpointProfile;
  }

  public UserProfile userProfile() {
    return userProfile;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", CompositeProfile.class.getSimpleName() + "[", "]")
        .add("serviceEndpointProfile=" + serviceEndpointProfile)
        .add("userProfile=" + userProfile)
        .toString();
  }
}
