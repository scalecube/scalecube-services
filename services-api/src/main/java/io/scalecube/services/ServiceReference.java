package io.scalecube.services;

import java.util.Collection;

public class ServiceReference {

  private final String endpointId;
  private final String serviceUri;
  private final Collection<ServiceRegistration> serviceRegistrations;

  public ServiceReference(String endpointId,
                          String serviceUri,
                          Collection<ServiceRegistration> serviceRegistrations) {
    this.endpointId = endpointId;
    this.serviceUri = serviceUri;
    this.serviceRegistrations = serviceRegistrations;
  }

  public String memberId() {
    return endpointId;
  }

  public String serviceUri() {
    return this.serviceUri;
  }

  public Collection<ServiceRegistration> serviceRegistrations() {
    return serviceRegistrations;
  }
}
