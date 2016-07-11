package io.scalecube.services;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import io.scalecube.cluster.ICluster;
import io.scalecube.services.annotations.AnnotationServiceProcessor;
import io.scalecube.services.state.DistributedState;
import io.scalecube.services.state.MemberState;
import io.scalecube.services.state.StateEvent;

import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.functions.Action1;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ServiceRegistry implements IServiceRegistry {

  // TODO [AK]: Add logging
  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceRegistry.class);

  private static final String SERVICE_REGISTRY_STATE_ID = "services";

  private final ICluster cluster;
  private final ServiceProcessor serviceProcessor;

  // Distributed service registry state
  private final DistributedState<ServiceRegistryRecord> serviceRegistryState;

  // Local indexes
  private final ConcurrentMap<String, Collection<ServiceReference>> servicesByNameIndex = new ConcurrentHashMap<>();
  private final ConcurrentMap<ServiceReference, ServiceInstance> serviceInstances = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, ServiceInstance> localServiceInstancesByName = new ConcurrentHashMap<>();

  public ServiceRegistry(ICluster cluster) {
    this(cluster, new AnnotationServiceProcessor());
  }

  public ServiceRegistry(ICluster cluster, ServiceProcessor serviceProcessor) {
    checkArgument(cluster != null);
    checkArgument(serviceProcessor != null);
    this.cluster = cluster;
    this.serviceProcessor = serviceProcessor;
    this.serviceRegistryState =
        new DistributedState<>(SERVICE_REGISTRY_STATE_ID, cluster, ServiceRegistryRecord.newInstance());
  }

  public void start() {
    serviceRegistryState.listenChanges().filter(StateEvent.ADDED_FILTER)
        .subscribe(new Action1<StateEvent<ServiceRegistryRecord>>() {
          @Override
          public void call(StateEvent<ServiceRegistryRecord> event) {
            onStateAddedEvent(event.newState().services());
          }
        });

    serviceRegistryState.listenChanges().filter(StateEvent.UPDATED_FILTER)
        .subscribe(new Action1<StateEvent<ServiceRegistryRecord>>() {
          @Override
          public void call(StateEvent<ServiceRegistryRecord> event) {
            onStateUpdatedEvent(event.oldState().services(), event.newState().services());
          }
        });

    serviceRegistryState.listenChanges().filter(StateEvent.REMOVED_FILTER)
        .subscribe(new Action1<StateEvent<ServiceRegistryRecord>>() {
          @Override
          public void call(StateEvent<ServiceRegistryRecord> event) {
            onStateRemovedEvent(event.oldState().services());
          }
        });

    serviceRegistryState.start();
  }

  private void onStateAddedEvent(Collection<ServiceReference> addedServices) {
    for (ServiceReference service : addedServices) {
      addRemoteService(service);
    }
  }

  private void onStateRemovedEvent(Collection<ServiceReference> removedServices) {
    for (ServiceReference service : removedServices) {
      removeRemoteService(service);
    }
  }

  private void onStateUpdatedEvent(Collection<ServiceReference> oldServices, Collection<ServiceReference> newServices) {
    // Remove unregistered or old version of modified services
    for (ServiceReference oldService : oldServices) {
      if (!newServices.contains(oldService)) {
        removeRemoteService(oldService);
      }
    }

    // Add registered or new version of modified services
    for (ServiceReference newService : newServices) {
      if (!oldServices.contains(newService)) {
        addRemoteService(newService);
      }
    }
  }

  private void addRemoteService(ServiceReference service) {
    ServiceInstance serviceInstance = new RemoteServiceInstance(cluster, service);
    serviceInstances.put(service, serviceInstance);
    addServiceByNameIndex(service); // index should be added after instance saved
  }

  private void removeRemoteService(ServiceReference service) {
    removeServiceByNameIndex(service); // index should be removed before instance saved
    serviceInstances.remove(service);
  }

  public void registerService(Object serviceObject) {
    checkArgument(serviceObject != null, "Service object can't be null.");
    Collection<Class<?>> serviceInterfaces = serviceProcessor.extractServiceInterfaces(serviceObject);
    Set<ServiceReference> registeredServiceReferences = new HashSet<>();
    for (Class<?> serviceInterface : serviceInterfaces) {
      // Process service interface
      ServiceDefinition serviceDefinition = serviceProcessor.introspectServiceInterface(serviceInterface);
      ServiceReference serviceReference = toLocalServiceReference(serviceDefinition);
      ServiceInstance serviceInstance = new LocalServiceInstance(serviceObject, serviceDefinition);

      // Update local indexes
      ServiceInstance prev = localServiceInstancesByName.putIfAbsent(serviceInstance.serviceName(), serviceInstance);
      checkState(prev == null, "Service with name %s was already registered", serviceInstance.serviceName());
      serviceInstances.put(serviceReference, serviceInstance);
      addServiceByNameIndex(serviceReference);

      registeredServiceReferences.add(serviceReference);
    }

    // Update distributed state
    if (!registeredServiceReferences.isEmpty()) {
      boolean succeed;
      do {
        MemberState<ServiceRegistryRecord> currentLocalState = serviceRegistryState.getLocalState();
        ServiceRegistryRecord currentLocalServices = currentLocalState.state();
        ServiceRegistryRecord newLocalServices = currentLocalServices.addAll(registeredServiceReferences);
        succeed = serviceRegistryState.compareAndSetLocalState(currentLocalState, newLocalServices);
      } while (!succeed);
    }
  }

  @Override
  public void unregisterService(Object serviceObject) {
    checkArgument(serviceObject != null, "Service object can't be null.");
    Collection<Class<?>> serviceInterfaces = serviceProcessor.extractServiceInterfaces(serviceObject);
    Set<ServiceReference> unregisteredServiceReferences = new HashSet<>();
    for (Class<?> serviceInterface : serviceInterfaces) {
      // Process service interface
      ServiceDefinition serviceDefinition = serviceProcessor.introspectServiceInterface(serviceInterface);
      ServiceReference serviceReference = toLocalServiceReference(serviceDefinition);

      // Update local indexes
      // TODO [AK]: check that it was not removed before
      removeServiceByNameIndex(serviceReference);
      localServiceInstancesByName.remove(serviceReference.serviceName());
      serviceInstances.remove(serviceReference);

      unregisteredServiceReferences.add(serviceReference);
    }

    // Update distributed state
    if (!unregisteredServiceReferences.isEmpty()) {
      boolean succeed;
      do {
        MemberState<ServiceRegistryRecord> currentLocalState = serviceRegistryState.getLocalState();
        ServiceRegistryRecord currentLocalServices = currentLocalState.state();
        ServiceRegistryRecord newLocalServices = currentLocalServices.removeAll(unregisteredServiceReferences);
        succeed = serviceRegistryState.compareAndSetLocalState(currentLocalState, newLocalServices);
      } while (!succeed);
    }
  }

  private ServiceReference toLocalServiceReference(ServiceDefinition serviceDefinition) {
    return new ServiceReference(cluster.membership().localMember().id(), serviceDefinition.serviceName(), serviceDefinition.methodNames());
  }

  @Override
  public Collection<ServiceReference> serviceLookup(final String serviceName) {
    checkArgument(serviceName != null, "Service name can't be null");
    Collection<ServiceReference> services = servicesByNameIndex.get(serviceName);
    return services == null ? Collections.<ServiceReference>emptySet() : Collections.unmodifiableCollection(services);
  }

  @Override
  public ServiceInstance serviceInstance(ServiceReference serviceReference) {
    return serviceInstances.get(serviceReference);
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

}
