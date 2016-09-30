package io.scalecube.services;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import io.scalecube.cluster.ICluster;
import io.scalecube.services.annotations.AnnotationServiceProcessor;

public class ServiceRegistry implements IServiceRegistry {

  // TODO [AK]: Add logging
  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceRegistry.class);

  private final ICluster cluster;
  private final ServiceProcessor serviceProcessor;
  private ConsulServiceRegistry consul;

  // Local indexes
  private final ConcurrentMap<String, Collection<ServiceReference>> servicesByNameIndex = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, ServiceInstance> localServiceInstancesByName = new ConcurrentHashMap<>();
  
  private final ConcurrentMap<ServiceReference, ServiceInstance> serviceInstances = new ConcurrentHashMap<>();
  
  ScheduledExecutorService exec;

  public ServiceRegistry(ICluster cluster) {
    this(cluster, new AnnotationServiceProcessor());
  }

  public ServiceRegistry(ICluster cluster, ServiceProcessor serviceProcessor) {
    checkArgument(cluster != null);
    checkArgument(serviceProcessor != null);
    this.cluster = cluster;
    this.serviceProcessor = serviceProcessor;
  }

  public void start() {
    consul = new ConsulServiceRegistry(cluster, "localhost");
    loadServices();
    exec = Executors.newScheduledThreadPool(1);
    exec.scheduleAtFixedRate(() -> loadServices(), 10, 10, TimeUnit.SECONDS);
  }

  private void loadServices() {
    for (RemoteServiceInstance instancef : consul.getRemoteServices()) {
      addServiceByNameIndex(instancef.serviceReference());
      serviceInstances.put(instancef.serviceReference(), instancef);
    }
    registerServices();
  }

  private void registerServices() {
    for (Entry<ServiceReference, ServiceInstance> entry : serviceInstances.entrySet()) {
      if (cluster.member().id().equals(entry.getValue().memberId())) {
        consul.registerService(cluster.member().id(),
            entry.getKey().serviceName(),
            cluster.member().address().host(),
            cluster.member().address().port());
      }
    }
  }

  public void registerService(Object serviceObject) {
    checkArgument(serviceObject != null, "Service object can't be null.");
    Collection<Class<?>> serviceInterfaces = serviceProcessor.extractServiceInterfaces(serviceObject);
    Set<ServiceReference> registeredServiceReferences = new HashSet<>();
    for (Class<?> serviceInterface : serviceInterfaces) {
      // Process service interface
      ConcurrentMap<String, ServiceDefinition> serviceDefinitions =
          serviceProcessor.introspectServiceInterface(serviceInterface);
      for (ServiceDefinition serviceDefinition : serviceDefinitions.values()) {
        ServiceReference serviceReference = toLocalServiceReference(serviceDefinition);
        ServiceInstance serviceInstance = new LocalServiceInstance(serviceObject, serviceDefinition, cluster.member().id());

        // Update local indexes
        ServiceInstance prev = localServiceInstancesByName.putIfAbsent(serviceInstance.serviceName(), serviceInstance);
        checkState(prev == null, "Service with name %s was already registered", serviceInstance.serviceName());
        serviceInstances.put(serviceReference, serviceInstance);
        addServiceByNameIndex(serviceReference);

        registeredServiceReferences.add(serviceReference);
        consul.registerService(cluster.member().id(),
            serviceReference.serviceName(),
            cluster.member().address().host(),
            cluster.member().address().port());
      }
    }
  }

  @Override
  public void unregisterService(Object serviceObject) {
    checkArgument(serviceObject != null, "Service object can't be null.");
    Collection<Class<?>> serviceInterfaces = serviceProcessor.extractServiceInterfaces(serviceObject);
    Set<ServiceReference> unregisteredServiceReferences = new HashSet<>();
    for (Class<?> serviceInterface : serviceInterfaces) {
      // Process service interface
      ConcurrentMap<String, ServiceDefinition> serviceDefinitions =
          serviceProcessor.introspectServiceInterface(serviceInterface);
      for (ServiceDefinition serviceDefinition : serviceDefinitions.values()) {
        ServiceReference serviceReference = toLocalServiceReference(serviceDefinition);

        // Update local indexes
        // TODO [AK]: check that it was not removed before
        removeServiceByNameIndex(serviceReference);
        localServiceInstancesByName.remove(serviceReference.serviceName());
        serviceInstances.remove(serviceReference);

        unregisteredServiceReferences.add(serviceReference);
      }
    }
  }

  private ServiceReference toLocalServiceReference(ServiceDefinition serviceDefinition) {
    return new ServiceReference(cluster.member().id(), serviceDefinition.serviceName());
  }

  @Override
  public Collection<ServiceReference> serviceLookup(final String serviceName) {
    checkArgument(serviceName != null, "Service name can't be null");
    Collection<ServiceReference> services = servicesByNameIndex.get(serviceName);
    
    return services == null ? Collections.<ServiceReference>emptySet() : Collections.unmodifiableCollection(services);
  }

  public ServiceInstance serviceInstance(ServiceReference reference) {
    return serviceInstances.get(reference);
  }

  @Override
  public ServiceInstance localServiceInstance(String serviceName) {
    return localServiceInstancesByName.get(serviceName);
  }

  private void addServiceByNameIndex(ServiceReference service) {
    String serviceName = service.serviceName();
    Collection<ServiceReference> services = getOrCreateServicesByNameIndex(serviceName);
    services.add(service);
  }

  private void removeServiceByNameIndex(ServiceReference service) {
    String serviceName = service.serviceName();
    Collection<ServiceReference> services = getOrCreateServicesByNameIndex(serviceName);
    services.remove(service);
  }

  private Collection<ServiceReference> getOrCreateServicesByNameIndex(String serviceName) {
    Collection<ServiceReference> services = servicesByNameIndex.get(serviceName);
    if (services == null) {
      services = Sets.newConcurrentHashSet();
      Collection<ServiceReference> fasterServices = servicesByNameIndex.putIfAbsent(serviceName, services);
      if (fasterServices != null) {
        services = fasterServices;
      }
    }
    return services;
  }

  public ServiceInstance remoteServiceInstance(String serviceName) {
    return serviceInstances.get(serviceName);
  }

  @Override
  public ServiceInstance findRemoteInstance(String serviceName) {
    return serviceInstances.get(serviceName);
  }
}
