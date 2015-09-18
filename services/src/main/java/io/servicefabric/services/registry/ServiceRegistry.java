package io.servicefabric.services.registry;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.geom.CubicCurve2D;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import io.servicefabric.cluster.ICluster;
import io.servicefabric.services.MultimapCache;
import io.servicefabric.services.annotations.ServiceAnnotationsProcessor;

public class ServiceRegistry implements IServiceRegistry {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceRegistry.class);

  private MultimapCache<String, ServiceReference> serviceRegistryCache = new MultimapCache<>();

  private final ConcurrentMap<String, ServiceInstance> localServices = new ConcurrentHashMap();

  private final ICluster cluster;

  public ServiceRegistry(ICluster cluster) {
    this.cluster = cluster;
  }

  public void start() {
   
	String localMemberId = cluster.membership().localMember().id();
	for(ServiceInstance serviceInstance : localServices.values()){
	   ServiceReference serviceReference  = new ServiceReference(serviceInstance.getServiceName() , localMemberId);	 
	   serviceRegistryCache.put(serviceInstance.getServiceName(), serviceReference);
	   // TODO: send gossip about service registration
	}
	
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
  }
  
  @Override
  public Collection<ServiceReference> serviceLookup(final String serviceName) {
    checkArgument(serviceName != null, "Service name can't be null");
    Collection<ServiceReference> serviceReferences;
    serviceReferences = serviceRegistryCache.get(serviceName);
    return serviceReferences == null ? Collections.<ServiceReference>emptySet() : serviceReferences;
  }
  
  public ServiceInstance getServiceInstance(String serviceName){
	  return localServices.get(serviceName);
  }
}
