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
import io.scalecube.cluster.Member;
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
    listenCluster();
  }

  private void listenCluster() {
    cluster.listenMembership().subscribe(event -> {
      if(event.isAdded()){
        loadMemberServices(DiscoveryType.ADDED, event.member());
      } else if (event.isRemoved()){
        loadMemberServices(DiscoveryType.RMOVED, event.member());
      }
    });
    
    Executors.newScheduledThreadPool(1).scheduleAtFixedRate(
        () -> loadClusterServices(), 10, 10, TimeUnit.SECONDS);

  }

  private void loadClusterServices() {
    cluster.otherMembers().forEach(member -> {
      loadMemberServices(DiscoveryType.DISCOVERED, member);
    });
  }

  private enum DiscoveryType{
    ADDED, RMOVED, DISCOVERED
  } 
  private void loadMemberServices(DiscoveryType type, Member member) {
    
    member.metadata().entrySet().stream().forEach(qualifier -> {
      ServiceReference serviceRef = new ServiceReference(
          member.id(),
          qualifier.getKey(),
          member.address(),
          new String[0]);
      LOGGER.debug("Member: {} is {} : {}", member,type, serviceRef );
      if (type.equals(DiscoveryType.ADDED) || type.equals(DiscoveryType.DISCOVERED)) {
        serviceInstances.putIfAbsent(serviceRef, new RemoteServiceInstance(cluster, serviceRef));
      } else if(type.equals(DiscoveryType.RMOVED)){
        serviceInstances.remove(serviceRef);
      }
    });
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
            ref.tags()),
            ref);
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
          LOGGER.debug("discovery register services {}", entry);
          discovery.registerService(ServiceRegistration.create(cluster.member().id(),
              entry.qualifier(),
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

        ServiceReference serviceRef = ServiceReference.create(
            definition, memberId, cluster.address(), tags);

        ServiceInstance serviceDef = ServiceDefinition.toLocalServiceInstance(
            definition, serviceObject, memberId, tags, definition.returnType());

        serviceInstances.putIfAbsent(serviceRef, serviceDef);

        if (discovery != null)
          discovery.registerService(
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
        ServiceReference serviceReference = toLocalServiceReference(serviceDefinition, null);
        serviceInstances.remove(serviceReference);
      });

    });
  }

  @Override
  public Collection<ServiceInstance> serviceLookup(final String qualifier) {
    checkArgument(qualifier != null, "Service qualifier can't be null");

    return serviceInstances.entrySet().stream()
        .filter(entry -> isValid(entry.getKey(), qualifier))
        .map(Map.Entry::getValue)
        .collect(Collectors.toList());
  }


  @Override
  public ServiceInstance getLocalInstance(String serviceName) {

    return serviceInstances.values().stream()
        .filter(entry -> entry.isLocal())
        .filter(entry -> entry.qualifier().equals(serviceName))
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
        tags);
  }

  private boolean isValid(ServiceReference reference, String qualifier) {
    return reference.qualifier().equals(qualifier);
  }
}
