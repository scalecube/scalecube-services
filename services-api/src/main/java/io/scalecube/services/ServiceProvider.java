package io.scalecube.services;

import java.util.Collection;

@FunctionalInterface
public interface ServiceProvider {

  Collection<ServiceInfo> provide(ServiceCall call);
}
