package io.scalecube.services;

import static java.util.Objects.requireNonNull;

import io.scalecube.cluster.Member;
import io.scalecube.concurrency.ThreadFactory;
import io.scalecube.services.ServicesConfig.Builder.ServiceConfig;
import io.scalecube.services.metrics.Metrics;
import io.scalecube.transport.Address;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ServiceRegistryImpl implements ServiceRegistry {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceRegistryImpl.class);

  private enum DiscoveryType {
    ADDED, REMOVED, DISCOVERED
  }

  private final ConcurrentMap<ServiceReference, ServiceInstance> serviceInstances = new ConcurrentHashMap<>();

  private final ConcurrentMap<String, ServiceDefinition> definitionsCache = new ConcurrentHashMap<>();

  private Microservices microservices;

  private Metrics metrics;

  Collection<ServiceInstance> localService = new ArrayList<ServiceInstance>();

  /**
   * the ServiceRegistry constructor to register and lookup cluster instances.
   *
   * @param microservices instance related to the service registry.
   * @param services services if relevant to this instance.
   * @param metrics nul1lable metrics factory if relevant to this instance.
   */
  public ServiceRegistryImpl(Microservices microservices, Metrics metrics) {
    requireNonNull(microservices != null, "microservices can't be null");
    this.microservices = microservices;
    this.metrics = metrics;
  }

  public ServiceRegistryImpl(Microservices microservices) {
    this(microservices,  null);
  }

  private void listenCluster() {
    microservices.cluster().listenMembership().subscribe(event -> {
      if (event.isAdded()) {
        loadMemberServices(DiscoveryType.ADDED, event.member());
      } else if (event.isRemoved()) {
        loadMemberServices(DiscoveryType.REMOVED, event.member());
      }
    });
    ThreadFactory.singleScheduledExecutorService("service-registry-discovery")
        .scheduleAtFixedRate(this::loadClusterServices, 10, 10, TimeUnit.SECONDS);
  }

  private void loadClusterServices() {
    this.microservices.cluster().otherMembers().forEach(member -> {
      loadMemberServices(DiscoveryType.DISCOVERED, member);
    });
  }

  private void loadMemberServices(DiscoveryType type, Member member) {
    member.metadata().entrySet().stream()
        .filter(entry -> "service".equals(entry.getValue())) // filter service tags
        .forEach(entry -> {
          Address serviceAddress = getServiceAddress(member);
          ServiceInfo info = ServiceInfo.from(entry.getKey());
          ServiceReference serviceRef = new ServiceReference(
              member.id(),
              info.getServiceName(),
              info.methods(),
              serviceAddress);

          LOGGER.debug("Member: {} is {} : {}", member, type, serviceRef);
          if (type.equals(DiscoveryType.ADDED) || type.equals(DiscoveryType.DISCOVERED)) {
            if (!serviceInstances.containsKey(serviceRef)) {
              serviceInstances.putIfAbsent(serviceRef,
                  new RemoteServiceInstance(microservices.client(), serviceRef, info.getTags()));
              LOGGER.info("Service Reference was ADDED since new Member has joined the cluster {} : {}", member,
                  serviceRef);
            }
          } else if (type.equals(DiscoveryType.REMOVED)) {
            serviceInstances.remove(serviceRef);
            LOGGER.info("Service Reference was REMOVED since Member have left the cluster {} : {}", member,
                serviceRef);
          }
        });
  }

  /**
   * register a service instance at the cluster.
   */
  public void registerService(ServiceConfig serviceCfg) {
    requireNonNull(serviceCfg != null, "Service object can't be null.");
    Collection<Class<?>> serviceInterfaces = Reflect.serviceInterfaces(serviceCfg.getService());

    String memberId = microservices.cluster().member().id();

    serviceInterfaces.forEach(serviceInterface -> {
      // Process service interface
      ServiceDefinition serviceDefinition = ServiceDefinition.from(serviceInterface);

      // cache the service definition.
      definitionsCache.putIfAbsent(serviceDefinition.serviceName(), serviceDefinition);

      ServiceReference serviceRef =
          new ServiceReference(memberId, serviceDefinition.serviceName(), serviceDefinition.methods().keySet(),
              microservices.serviceAddress());

      ServiceInstance serviceInstance =
          new LocalServiceInstance(serviceCfg.getService(),
              serviceCfg.getTags(),
              microservices.serviceAddress(), 
              memberId,
              serviceDefinition.serviceName(),
              serviceDefinition.methods(),
              this.metrics);

      serviceInstances.putIfAbsent(serviceRef, serviceInstance);
      localService = serviceInstances.values();
    });
  }

  @Override
  public List<ServiceInstance> serviceLookup(final String serviceName) {
    requireNonNull(serviceName != null, "Service name can't be null");
    return Collections.unmodifiableList(serviceInstances.entrySet().stream()
        .filter(entry -> isValid(entry.getKey(), serviceName))
        .map(Map.Entry::getValue)
        .collect(Collectors.toList()));
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

  private Address getServiceAddress(Member member) {
    String serviceAddressAsString = member.metadata().get("service-address");
    if (serviceAddressAsString != null) {
      return Address.from(serviceAddressAsString);
    } else {
      return member.address();
    }
  }

  private boolean isValid(ServiceReference reference, String qualifier) {
    return reference.serviceName().equals(qualifier);
  }

  @Override
  public void start() {
    loadClusterServices();
    listenCluster();
  }

  @Override
  public Collection<ServiceInstance> localServices() {
    return localService;
  }

}
