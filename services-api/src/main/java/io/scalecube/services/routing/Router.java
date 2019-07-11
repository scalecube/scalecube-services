package io.scalecube.services.routing;

import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.registry.api.ServiceRegistry;
import java.util.Optional;
import reactor.util.annotation.Nullable;

@FunctionalInterface
public interface Router {

  /**
   * Returns suitable service references for a given request message.
   *
   * @param serviceRegistry service registry (optional)
   * @param request service message
   * @return service instance (optional)
   */
  Optional<ServiceReference> route(
      @Nullable ServiceRegistry serviceRegistry, ServiceMessage request);
}
