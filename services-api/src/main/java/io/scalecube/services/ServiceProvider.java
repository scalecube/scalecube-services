package io.scalecube.services;

import java.util.Collection;

/**
 * Provide service instances.
 *
 * @deprecated use {@link ServicesProvider}
 */
@FunctionalInterface
@Deprecated
public interface ServiceProvider {

  Collection<ServiceInfo> provide(ServiceCall call);
}
