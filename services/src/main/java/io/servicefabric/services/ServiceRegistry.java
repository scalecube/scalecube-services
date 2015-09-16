package io.servicefabric.services;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;

import io.servicefabric.cluster.ClusterMember;
import io.servicefabric.cluster.ICluster;
import io.servicefabric.services.annotations.ServiceAnnotationsProcessor;

public class ServiceRegistry implements IServiceRegistry {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceRegistry.class);

  private MultimapCache<String, ServiceReference> serviceRegistryCache = new MultimapCache<>();
  private final Multimap<String, ServiceReference> localServices = HashMultimap.create();

  private final ICluster cluster;

  public ServiceRegistry(ICluster cluster) {
    this.cluster = cluster;
  }

  public void start() {
    // TODO : do I need to clean registration of services for removed members or just clean up them periodically?
    // TODO : listen for gossips of service registration
    // TODO : send sync events
  }

  @Override
  public void registerService(String serviceName) {
    ServiceReference serviceReference = resolveReference(serviceName);
    // Register service locally
    localServices.put(serviceName, serviceReference);
    // TODO: send gossip about service registration
    serviceRegistryCache.put(serviceReference.name(), serviceReference);
  }

  public void registerService(Object serviceInstance) {
    new ServiceAnnotationsProcessor().registerService(serviceInstance);
  }

  @Override
  public Collection<ServiceReference> serviceLookup(final String serviceName) {
    checkArgument(serviceName != null, "Service name can't be null");
    Collection<ServiceReference> serviceReferences;
    serviceReferences = serviceRegistryCache.get(serviceName);
    return serviceReferences == null ? Collections.<ServiceReference>emptySet() : serviceReferences;
  }

  private ServiceReference resolveReference(String serviceName) {
    checkArgument(serviceName != null, "Service namespace should not be null");
    ClusterMember localMember = cluster.membership().localMember();
    return new ServiceReference(serviceName, localMember.id());
  }

}
