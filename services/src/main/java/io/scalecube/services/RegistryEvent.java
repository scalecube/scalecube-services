package io.scalecube.services;

import static com.google.common.base.Preconditions.checkArgument;

public class RegistryEvent {

  private final ServiceReference serviceReference;

  public ServiceReference serviceReference() {
    return serviceReference;
  }

  public ServiceInstance serviceInstance() {
    return serviceInstance;
  }

  private final ServiceInstance serviceInstance;

  protected RegistryEvent(ServiceReference serviceReference, ServiceInstance serviceInstance) {
    this.serviceReference = serviceReference;
    this.serviceInstance = serviceInstance;
  }

  public static RegistryEvent create(ServiceReference serviceReference, ServiceInstance serviceInstance) {
    checkArgument(serviceReference != null);
    checkArgument(serviceInstance != null);
    return new RegistryEvent(serviceReference, serviceInstance);
  }

}
