package io.scalecube.services;

import io.scalecube.services.ServiceCall.Call;
import java.util.Collection;

@FunctionalInterface
public interface ServiceProvider {

  Collection<ServiceInfo> provide(Call call);
}
