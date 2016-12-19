package io.scalecube.services;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.cluster.ICluster;
import io.scalecube.cluster.Member;
import io.scalecube.services.ServicesConfig.Builder.ServiceConfig;
import io.scalecube.services.annotations.ServiceProcessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ServiceRegistryImpl implements ServiceRegistry {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceRegistryImpl.class);

  private enum DiscoveryType {
    ADDED, REMOVED, DISCOVERED
  }

  private final ICluster cluster;
  private final ServiceProcessor serviceProcessor;

  private final ConcurrentMap<ServiceReference, ServiceInstance> serviceInstances = new ConcurrentHashMap<>();

  private final ConcurrentMap<String, ServiceDefinition> definitionsCache = new ConcurrentHashMap<>();

  /**
   * the ServiceRegistry constructor to register and lookup cluster instances.
   *
   * @param cluster the cluster instance related to the service registry.
   * @param services optional services if relevant to this instance.
   * @param serviceProcessor - service processor.
   * @param isSeed indication if this member is seed.
   */
  public ServiceRegistryImpl(ICluster cluster, ServicesConfig services, ServiceProcessor serviceProcessor,
      boolean isSeed) {
    checkArgument(cluster != null);
    checkArgument(services != null);
    checkArgument(serviceProcessor != null);

    this.serviceProcessor = serviceProcessor;
    this.cluster = cluster;
    CompletableFuture<Void> future = listenCluster();

    if (!services.services().isEmpty()) {
      for (ServiceConfig service : services.services()) {
        registerService(service);
      }
    }

    if (!isSeed && cluster.otherMembers().isEmpty()) {
      try {
        future.get();
      } catch (Exception ex) {
        LOGGER.error("error while waiting to join the cluster members event", ex);
      }
    } else {
      loadClusterServices();
    }
  }

  private CompletableFuture<Void> listenCluster() {
    CompletableFuture<Void> future = new CompletableFuture<>();

    cluster.listenMembership().subscribe(event -> {
      if (event.isAdded()) {
        loadMemberServices(DiscoveryType.ADDED, event.member());
      } else if (event.isRemoved()) {
        loadMemberServices(DiscoveryType.REMOVED, event.member());
      }
      if (!cluster.members().isEmpty()) {
        future.complete(null);
      }
    });

    Executors.newScheduledThreadPool(1).scheduleAtFixedRate(this::loadClusterServices, 10, 10, TimeUnit.SECONDS);

    return future;
  }

  private void loadClusterServices() {
    cluster.otherMembers().forEach(member -> {
      loadMemberServices(DiscoveryType.DISCOVERED, member);
    });
  }

  private void loadMemberServices(DiscoveryType type, Member member) {
    member.metadata().entrySet().stream()
        .filter(entry -> "service".equals(entry.getValue())) // filter service tags
        .forEach(entry -> {
          ServiceInfo info = ServiceInfo.from(entry.getKey());
          ServiceReference serviceRef = new ServiceReference(
              member.id(),
              info.getServiceName(),
              member.address());

          LOGGER.debug("Member: {} is {} : {}", member, type, serviceRef);
          if (type.equals(DiscoveryType.ADDED) || type.equals(DiscoveryType.DISCOVERED)) {
            serviceInstances.putIfAbsent(serviceRef, new RemoteServiceInstance(this, serviceRef, info.getTags()));
          } else if (type.equals(DiscoveryType.REMOVED)) {
            serviceInstances.remove(serviceRef);
          }
        });
  }

  /**
   * register a service instance at the cluster.
   */
  public void registerService(ServiceConfig serviceObject) {
    checkArgument(serviceObject != null, "Service object can't be null.");
    Collection<Class<?>> serviceInterfaces = serviceProcessor.extractServiceInterfaces(serviceObject.getService());

    String memberId = cluster.member().id();

    serviceInterfaces.forEach(serviceInterface -> {
      // Process service interface
      ServiceDefinition serviceDefinition =
          serviceProcessor.introspectServiceInterface(serviceInterface);

      // cache the service definition.
      definitionsCache.putIfAbsent(serviceDefinition.serviceName(), serviceDefinition);

      ServiceReference serviceRef = new ServiceReference(memberId, serviceDefinition.serviceName(), cluster.address());

      ServiceInstance serviceInstance =
          new LocalServiceInstance(serviceObject, memberId, serviceDefinition.serviceName(),
              serviceDefinition.methods());
      serviceInstances.putIfAbsent(serviceRef, serviceInstance);

    });
  }

  @Override
  public void unregisterService(Object serviceObject) {
    checkArgument(serviceObject != null, "Service object can't be null.");
    Collection<Class<?>> serviceInterfaces = serviceProcessor.extractServiceInterfaces(serviceObject);

    serviceInterfaces.forEach(serviceInterface -> {
      // Process service interface
      ServiceDefinition serviceDefinition =
          serviceProcessor.introspectServiceInterface(serviceInterface);

      ServiceReference serviceReference = toLocalServiceReference(serviceDefinition);
      serviceInstances.remove(serviceReference);

    });
  }

  @Override
  public List<ServiceInstance> serviceLookup(final String serviceName) {
    checkArgument(serviceName != null, "Service name can't be null");
    return serviceInstances.entrySet().stream()
        .filter(entry -> isValid(entry.getKey(), serviceName))
        .map(Map.Entry::getValue)
        .collect(Collectors.toList());
  }

  @Override
  public Optional<ServiceInstance> getLocalInstance(String serviceName, String method) {
    return serviceInstances.values().stream()
        .filter(ServiceInstance::isLocal)
        .filter(serviceInstance -> serviceInstance.serviceName().equals(serviceName))
        .findFirst();
  }

  public Collection<ServiceInstance> services() {
    return Collections.unmodifiableCollection(serviceInstances.values());
  }

  private ServiceReference toLocalServiceReference(ServiceDefinition serviceDefinition) {
    return new ServiceReference(cluster.member().id(), serviceDefinition.serviceName(), cluster.address());
  }

  private boolean isValid(ServiceReference reference, String qualifier) {
    return reference.serviceName().equals(qualifier);
  }

  @Override
  public Optional<ServiceDefinition> getServiceDefinition(String serviceName) {
    return Optional.ofNullable(definitionsCache.get(serviceName));
  }

  @Override
  public ICluster cluster() {
    return this.cluster;
  }

  @Override
  public ServiceDefinition registerInterface(Class<?> serviceInterface) {
    ServiceDefinition serviceDefinition = serviceProcessor.introspectServiceInterface(serviceInterface);
    definitionsCache.putIfAbsent(serviceDefinition.serviceName(), serviceDefinition);
    return serviceDefinition;
  }
}
