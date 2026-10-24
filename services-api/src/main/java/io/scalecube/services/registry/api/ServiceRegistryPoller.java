package io.scalecube.services.registry.api;

import io.scalecube.services.ServiceEndpoint;
import java.util.Collection;
import java.util.function.Consumer;

/**
 * Polls the changes of {@link ServiceRegistry#listServiceEndpoints()}: the endpoints added and the
 * endpoints removed since the previous poll. The first poll reports every registered endpoint as
 * added. Reads the endpoints only when {@link ServiceRegistry#version()} changed, so a poll without
 * changes does not allocate.
 *
 * <p>Not thread-safe: a poller belongs to the thread that polls it (e.g. an agent's duty cycle),
 * the registry itself is shared.
 */
public interface ServiceRegistryPoller {

  /**
   * Reports the endpoints removed, then the endpoints added, since the previous poll. When a
   * callback throws, the exception propagates and the changes not yet reported by this poll are
   * lost for this poller.
   *
   * @param onAdded called for each added endpoint
   * @param onRemoved called for each removed endpoint
   * @return number of changes reported, {@code 0} if none
   */
  int poll(Consumer<ServiceEndpoint> onAdded, Consumer<ServiceEndpoint> onRemoved);

  /**
   * Polls the changes without callbacks, e.g. to re-read {@link #endpoints()} only on a change.
   *
   * @return number of changes, {@code 0} if none
   */
  default int poll() {
    return poll(endpoint -> {}, endpoint -> {});
  }

  /**
   * Returns the endpoints as of the last poll, already updated while its callbacks run.
   *
   * @return endpoints (unmodifiable)
   */
  Collection<ServiceEndpoint> endpoints();
}
