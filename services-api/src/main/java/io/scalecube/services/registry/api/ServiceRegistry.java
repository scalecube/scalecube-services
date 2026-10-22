package io.scalecube.services.registry.api;

import io.scalecube.services.ServiceCall;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.ServiceProvider;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.methods.ServiceMethodInvoker;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import reactor.core.scheduler.Scheduler;

/**
 * Service registry interface provides API to register/unregister services in the system and make
 * services lookup by service result.
 */
public interface ServiceRegistry {

  /**
   * Returns list of {@link ServiceEndpoint} objects that was registered locally.
   *
   * @return list of {@link ServiceEndpoint} objects
   */
  List<ServiceEndpoint> listServiceEndpoints();

  /**
   * Returns version of the {@link ServiceEndpoint} objects, incremented after every change of
   * {@link #listServiceEndpoints()}. A reader that reads the version before the endpoints sees
   * every change: either in that read, or by a version different from the one it read.
   *
   * @return version of the {@link ServiceEndpoint} objects
   */
  long version();

  /**
   * Returns new {@link Poller} of the {@link ServiceEndpoint} objects of this registry.
   *
   * @return poller
   */
  Poller poller();

  /**
   * Returns list of {@link ServiceReference} objects that was registered locally.
   *
   * @return list of {@link ServiceReference} objects
   */
  List<ServiceReference> listServiceReferences();

  /**
   * Looking up list of {@link ServiceReference} objects.
   *
   * @param request request
   * @return list of {@link ServiceReference} objects
   */
  List<ServiceReference> lookupService(ServiceMessage request);

  /**
   * Registering {@link ServiceEndpoint} object that was received by discovery mechanism.
   *
   * @param serviceEndpoint serviceEndpoint
   * @see ServiceDiscovery#listen()
   */
  void registerService(ServiceEndpoint serviceEndpoint);

  /**
   * Registering locally defined {@link ServiceInfo} object.
   *
   * @param serviceInfo serviceInfo
   * @see ServiceProvider
   * @see ServiceProvider#provide(ServiceCall)
   */
  void registerService(ServiceInfo serviceInfo);

  /**
   * Registering locally defined {@link ServiceInfo} object.
   *
   * @param serviceInfo serviceInfo
   * @param schedulers schedulers map (nullable)
   * @param qualifierOperator qualifier operator (nullable)
   */
  void registerService(
      ServiceInfo serviceInfo,
      Map<String, Scheduler> schedulers,
      UnaryOperator<String> qualifierOperator);

  /**
   * Unregistering {@link ServiceEndpoint}.
   *
   * @param endpointId service endpoint id
   * @return {@link ServiceEndpoint} instance that was unregistered, or null
   */
  ServiceEndpoint unregisterService(String endpointId);

  /**
   * Returns locally defined {@link ServiceInfo} objects.
   *
   * @return {@link ServiceInfo} objects
   */
  List<ServiceInfo> listServices();

  /**
   * Looking up {@link ServiceMethodInvoker} by request.
   *
   * @param request request
   * @return {@link ServiceMethodInvoker} instance, or null
   */
  ServiceMethodInvoker lookupInvoker(ServiceMessage request);

  /**
   * Polls the changes of {@link ServiceRegistry#listServiceEndpoints()}: the endpoints added and
   * the endpoints removed since the previous poll. The first poll reports every registered endpoint
   * as added. Reads the endpoints only when {@link ServiceRegistry#version()} changed, so a poll
   * without changes does not allocate.
   *
   * <p>Not thread-safe: a poller belongs to the thread that polls it (e.g. an agent's duty cycle),
   * the registry itself is shared.
   */
  interface Poller {

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
     * Returns the endpoints as of the last poll, already updated while its callbacks run.
     *
     * @return endpoints (unmodifiable)
     */
    Collection<ServiceEndpoint> endpoints();
  }
}
