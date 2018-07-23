package io.scalecube.services.registry.api;

import io.scalecube.services.ServiceEndpoint;

/**
 * Service registration event. This event is being fired when {@link ServiceEndpoint} is being added (or removed from)
 * to (from) {@link ServiceRegistry}.
 */
public class RegistrationEvent {

  public enum Type {
    REGISTERED, // service endpoint added
    UNREGISTERED // service endpoint removed
  }

  private final ServiceEndpoint serviceEndpoint;
  private final Type type;

  private RegistrationEvent(Type type, ServiceEndpoint serviceEndpoint) {
    this.serviceEndpoint = serviceEndpoint;
    this.type = type;
  }

  public static RegistrationEvent registered(ServiceEndpoint serviceEndpoint) {
    return new RegistrationEvent(Type.REGISTERED, serviceEndpoint);
  }

  public static RegistrationEvent unregistered(ServiceEndpoint serviceEndpoint) {
    return new RegistrationEvent(Type.UNREGISTERED, serviceEndpoint);
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
