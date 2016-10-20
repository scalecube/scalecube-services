package io.scalecube.services;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.scalecube.cluster.ICluster;
import io.scalecube.services.annotations.AnnotationServiceProcessor;

public class ServiceRegistry implements IServiceRegistry {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceRegistry.class);

  private final ICluster cluster;
  private final ServiceProcessor serviceProcessor;
  private ServiceDiscovery discovery;

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

  public void start(ServiceDiscovery discovery) {
    this.discovery = discovery;
    loadServices();
    this.exec = Executors.newScheduledThreadPool(1);
    this.exec.scheduleAtFixedRate(() -> loadServices(), 10, 10, TimeUnit.SECONDS);
  }

  private void loadServices() {
    discovery.getRemoteServices().forEach(ref -> serviceInstances.put(ref.serviceReference(), ref));
    registerServices();
  }

  private void registerServices() {
    serviceInstances.entrySet().stream()
        .filter(entry -> cluster.member().id().equals(entry.getValue().memberId()))
        .forEach(entry -> {

          discovery.registerService(ServiceRegistration.create(cluster.member().id(),
              entry.getKey().serviceName(),
              cluster.member().address().host(),
              cluster.member().address().port()));
        });
  }


  public void registerService(Object serviceObject) {
    checkArgument(serviceObject != null, "Service object can't be null.");
    Collection<Class<?>> serviceInterfaces = serviceProcessor.extractServiceInterfaces(serviceObject);

    String memberId = cluster.member().id();

    serviceInterfaces.forEach(serviceInterface -> {
      // Process service interface
      ConcurrentMap<String, ServiceDefinition> serviceDefinitions =
          serviceProcessor.introspectServiceInterface(serviceInterface);

      serviceDefinitions.entrySet().stream().forEach(entry -> {
        serviceInstances.putIfAbsent(
            ServiceReference.create(entry.getValue(), memberId),
            new LocalServiceInstance(serviceObject, entry.getValue(), memberId));

        discovery.registerService(ServiceRegistration.create(
            memberId, entry.getValue().qualifier(),
            cluster.member().address().host(),
            cluster.member().address().port()));
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
        ServiceReference serviceReference = toLocalServiceReference(serviceDefinition);
        serviceInstances.remove(serviceReference);
      });

    });
  }

  private ServiceReference toLocalServiceReference(ServiceDefinition serviceDefinition) {
    return new ServiceReference(cluster.member().id(), serviceDefinition.qualifier());
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

  private boolean isValid(ServiceReference reference, String serviceName) {
    if (reference.serviceName().equals(serviceName)) {
      return cluster.member(reference.memberId()).isPresent();
    }
    return false;
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



}
