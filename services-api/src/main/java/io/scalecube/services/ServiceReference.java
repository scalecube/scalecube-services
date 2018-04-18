package io.scalecube.services;

import java.util.Collection;

public class ServiceReference {

  private final String endpointId;
  private final String host;
  private final String port;
  private final Collection<ServiceRegistration> serviceRegistrations;

  public ServiceReference(String endpointId,
      String host,
      String port,
      Collection<ServiceRegistration> serviceRegistrations) {
    this.endpointId = endpointId;
    this.host = host;
    this.port = port;
    this.serviceRegistrations = serviceRegistrations;
  }

  public String endpointId() {
    return endpointId;
  }

  public String host() {
    return host;
  }

  public String port() {
    return port;
  }

  public Collection<ServiceRegistration> serviceRegistrations() {
    return serviceRegistrations;
  }
}
