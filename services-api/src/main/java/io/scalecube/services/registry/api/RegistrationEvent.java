package io.scalecube.services.registry.api;

import io.scalecube.services.ServiceEndpoint;

public class RegistrationEvent {

  public enum Type {
    ADDED, REMOVED
  }

  private ServiceEndpoint serviceEndpoint;
  private Type type;

  public RegistrationEvent(Type type, ServiceEndpoint serviceEndpoint) {
    this.serviceEndpoint = serviceEndpoint;
    this.type = type;
  }

  public ServiceEndpoint serviceEndpoint() {
    return this.serviceEndpoint;
  }

  public Type type() {
    return this.type;
  }

  @Override
  public String toString() {
    return "RegistrationEvent [serviceEndpoint=" + serviceEndpoint + ", type=" + type + "]";
  }
}
