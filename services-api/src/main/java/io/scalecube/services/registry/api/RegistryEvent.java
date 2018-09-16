package io.scalecube.services.registry.api;

import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceReference;
import java.util.function.Predicate;

public interface RegistryEvent<T> {

  enum Type {
    ADDED,
    REMOVED;
  }

  static Predicate<RegistryEvent<ServiceReference>> asReference() {
    return registryEvent ->
        ((RegistryEvent) registryEvent).value().getClass().equals(ServiceReference.class);
  }

  static Predicate<RegistryEvent<ServiceEndpoint>> asEndpoint() {
    return registryEvent ->
        ((RegistryEvent) registryEvent).value().getClass().equals(ServiceEndpoint.class);
  }

  /**
   * Creates a registry event instance by the given args.
   *
   * @param type registry event type {@link Type}
   * @param value value
   * @return registry event instance
   */
  static <T> RegistryEvent<T> create(RegistryEvent.Type type, T value) {
    return new RegistryEvent<T>() {
      @Override
      public RegistryEvent.Type type() {
        return type;
      }

      @Override
      public T value() {
        return value;
      }
    };
  }

  RegistryEvent.Type type();

  T value();

  default boolean isAdded() {
    return RegistryEvent.Type.ADDED.equals(type());
  }

  default boolean isRemoved() {
    return RegistryEvent.Type.REMOVED.equals(type());
  }
}
