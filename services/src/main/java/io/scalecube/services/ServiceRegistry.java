package io.scalecube.services;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.scalecube.cluster.ICluster;
import io.scalecube.services.annotations.AnnotationServiceProcessor;
import io.scalecube.services.annotations.ServiceProcessor;

public class ServiceRegistry implements IServiceRegistry {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceRegistry.class);

  private final ICluster cluster;
  private final ServiceProcessor serviceProcessor;
  private ServiceDiscovery discovery;

  private final ConcurrentMap<ServiceReference, ServiceInstance> serviceInstances = new ConcurrentHashMap<>();

  public ServiceRegistry(ICluster cluster) {
    this(cluster, new AnnotationServiceProcessor());
  }

  public ServiceRegistry(ICluster cluster, ServiceProcessor serviceProcessor) {
    checkArgument(cluster != null);
    checkArgument(serviceProcessor != null);
    this.cluster = cluster;
    this.serviceProcessor = serviceProcessor;
  }

  public void start(ServiceDiscovery discovery) {
    this.discovery = discovery;
    loadServices();

    Executors.newScheduledThreadPool(1).scheduleAtFixedRate(
        () -> loadServices(), 10, 10, TimeUnit.SECONDS);

    Executors.newScheduledThreadPool(1).scheduleAtFixedRate(
        () -> discovery.cleanup(), 60, 60, TimeUnit.SECONDS);
  }

  private void loadServices() {
    try {
      registerServices();

      discovery.getRemoteServices().forEach(ref -> {
        serviceInstances.put(new ServiceReference(ref.memberId(), 
            ref.qualifier(), 
            ref.address(), 
            ref.tags()), ref);
      });
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  private void registerServices() {
    serviceInstances.keySet().stream()
        .filter(entry -> {
          return cluster.member().id().equals(entry.memberId());
        }).forEach(entry -> {
          discovery.registerService(ServiceRegistration.create(cluster.member().id(),
              entry.serviceName(),
              cluster.member().address().host(),
              cluster.member().address().port()));
        });
  }

  public void registerService(Object serviceObject, String[] tags) {
    checkArgument(serviceObject != null, "Service object can't be null.");
    Collection<Class<?>> serviceInterfaces = serviceProcessor.extractServiceInterfaces(serviceObject);

    String memberId = cluster.member().id();

    serviceInterfaces.forEach(serviceInterface -> {
      // Process service interface
      ConcurrentMap<String, ServiceDefinition> serviceDefinitions =
          serviceProcessor.introspectServiceInterface(serviceInterface);

      serviceDefinitions.values().stream().forEach(definition -> {
        
        serviceInstances.putIfAbsent(
            ServiceReference.create(definition, memberId, cluster.address(),tags),
            ServiceDefinition.toLocalServiceInstance(definition, serviceObject, memberId,tags));

        if(discovery!=null) discovery.registerService(
            ServiceRegistration.create(memberId, definition.qualifier(),
                cluster.member().address().host(),
                cluster.member().address().port(),
                tags));
      });
    });
  }

  @Override
  public void unregisterService(Object serviceObject) {
    checkArgument(serviceObject != null, "Service object can't be null.");
    Collection<Class<?>> serviceInterfaces = serviceProcessor.extractServiceInterfaces(serviceObject);

    serviceInterfaces.forEach(serviceInterface -> {

      // Process service interface
      ConcurrentMap<String, ServiceDefinition> serviceDefinitions =
          serviceProcessor.introspectServiceInterface(serviceInterface);

      serviceDefinitions.values().forEach(serviceDefinition -> {
        ServiceReference serviceReference = toLocalServiceReference(serviceDefinition,null);
        serviceInstances.remove(serviceReference);
      });

    });
  }

  @Override
  public Collection<ServiceInstance> serviceLookup(final String serviceName) {
    checkArgument(serviceName != null, "Service name can't be null");

    return serviceInstances.entrySet().stream()
        .filter(entry -> isValid(entry.getKey(), serviceName))
        .map(Map.Entry::getValue)
        .collect(Collectors.toList());
  }


  @Override
  public ServiceInstance getLocalInstance(String serviceName) {

    return serviceInstances.entrySet().stream()
        .filter(entry -> entry.getValue().isLocal())
        .map(Map.Entry::getValue)
        .findFirst().get();
  }

  public ServiceInstance serviceInstance(ServiceReference reference) {
    return serviceInstances.get(reference);
  }

  public ServiceInstance remoteServiceInstance(String serviceName) {
    return serviceInstances.get(serviceName);
  }

  @Override
  public List<RemoteServiceInstance> findRemoteInstance(String serviceName) {
    return discovery.serviceLookup(serviceName);
  }

  public Collection<ServiceInstance> services() {
    return serviceInstances.values();
  }

  private ServiceReference toLocalServiceReference(ServiceDefinition serviceDefinition, String[] tags) {
    return new ServiceReference(cluster.member().id(),
        serviceDefinition.qualifier(),
        cluster.address(),
        tags
        );
  }
  
  private boolean isValid(ServiceReference reference, String serviceName) {
    return reference.serviceName().equals(serviceName);
  }
}
