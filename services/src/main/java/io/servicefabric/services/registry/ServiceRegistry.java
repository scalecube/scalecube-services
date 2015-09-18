package io.servicefabric.services.registry;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;

import io.servicefabric.cluster.ICluster;
import io.servicefabric.services.MultimapCache;
import io.servicefabric.services.annotations.ServiceAnnotationsProcessor;

public class ServiceRegistry implements IServiceRegistry {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceRegistry.class);

  private MultimapCache<String, ServiceInstance> serviceRegistryCache = new MultimapCache<>();
  private final Multimap<String, ServiceInstance> localServices = HashMultimap.create();

  private final ICluster cluster;

  public ServiceRegistry(ICluster cluster) {
    this.cluster = cluster;
  }

  public void start() {
    // TODO : do I need to clean registration of services for removed members or just clean up them periodically?
    // TODO : listen for gossips of service registration
    // TODO : send sync events
  }

  public void registerService(Object serviceObject) {
    Collection<ServiceInstance> serviceInstances = ServiceAnnotationsProcessor.processService(serviceObject);
    for (ServiceInstance serviceInstance : serviceInstances) {
      registerService(serviceInstance);
    }
  }

  public void registerService(ServiceInstance serviceInstance) {
    localServices.put(serviceInstance.getServiceName(), serviceInstance);
    // TODO: send gossip about service registration
    serviceRegistryCache.put(serviceInstance.getServiceName(), serviceInstance);
  }

  @Override
  public Collection<ServiceInstance> serviceLookup(final String serviceName) {
    checkArgument(serviceName != null, "Service name can't be null");
    Collection<ServiceInstance> serviceReferences;
    serviceReferences = serviceRegistryCache.get(serviceName);
    return serviceReferences == null ? Collections.<ServiceInstance>emptySet() : serviceReferences;
  }



}
