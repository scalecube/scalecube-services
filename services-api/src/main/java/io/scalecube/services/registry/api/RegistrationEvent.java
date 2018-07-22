package io.scalecube.services.registry.api;

import io.scalecube.services.ServiceEndpoint;

public class RegistrationEvent {

  public enum RegistrationEventType {
    REGISTERED, UNREGISTERED
  }

  private ServiceEndpoint serviceEndpoint;
  private RegistrationEventType type;

  public RegistrationEvent(RegistrationEventType type, ServiceEndpoint serviceEndpoint) {
    this.serviceEndpoint = serviceEndpoint;
    this.type = type;
  }

  public ServiceEndpoint serviceEndpoint() {
    return this.serviceEndpoint;
  }

  public RegistrationEventType type() {
    return this.type;
  }

  @Override
  public String toString() {
    return "RegistrationEvent [serviceEndpoint=" + serviceEndpoint + ", type=" + type + "]";
  }
}
