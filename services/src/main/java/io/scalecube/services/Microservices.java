package io.scalecube.services;

import com.codahale.metrics.MetricRegistry;
import io.scalecube.services.discovery.ServiceScanner;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import io.scalecube.services.exceptions.DefaultErrorMapper;
import io.scalecube.services.exceptions.ServiceProviderErrorMapper;
import io.scalecube.services.gateway.Gateway;
import io.scalecube.services.gateway.GatewayOptions;
import io.scalecube.services.methods.ServiceMethodRegistry;
import io.scalecube.services.methods.ServiceMethodRegistryImpl;
import io.scalecube.services.metrics.Metrics;
import io.scalecube.services.registry.ServiceRegistryImpl;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.transport.api.Address;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.ServerTransport;
import io.scalecube.services.transport.api.TransportResources;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;

/**
 * The ScaleCube-Services module enables to provision and consuming microservices in a cluster.
 * ScaleCube-Services provides Reactive application development platform for building distributed
 * applications Using microservices and fast data on a message-driven runtime that scales
 * transparently on multi-core, multi-process and/or multi-machines Most microservices frameworks
 * focus on making it easy to build individual microservices. ScaleCube allows developers to run a
 * whole system of microservices from a single command. removing most of the boilerplate code,
 * ScaleCube-Services focuses development on the essence of the service and makes it easy to create
 * explicit and typed protocols that compose. True isolation is achieved through shared-nothing
 * design. This means the services in ScaleCube are autonomous, loosely coupled and mobile (location
 * transparent)—necessary requirements for resilance and elasticity ScaleCube services requires
 * developers only to two simple Annotations declaring a Service but not regards how you build the
 * service component itself. the Service component is simply java class that implements the service
 * Interface and ScaleCube take care for the rest of the magic. it derived and influenced by Actor
 * model and reactive and streaming patters but does not force application developers to it.
 * ScaleCube-Services is not yet-anther RPC system in the sense its is cluster aware to provide:
 *
 * <ul>
 *   <li>location transparency and discovery of service instances.
 *   <li>fault tolerance using gossip and failure detection.
 *   <li>share nothing - fully distributed and decentralized architecture.
 *   <li>Provides fluent, java 8 lambda apis.
 *   <li>Embeddable and lightweight.
 *   <li>utilizes completable futures but primitives and messages can be used as well completable
 *       futures gives the advantage of composing and chaining service calls and service results.
 *   <li>low latency
 *   <li>supports routing extensible strategies when selecting service end-points
 * </ul>
 *
 * <b>basic usage example:</b>
 *
 * <pre>{@code
 * // Define a service interface and implement it:
 * &#64; Service
 * public interface GreetingService {
 *      &#64; ServiceMethod
 *      Mono<String> sayHello(String string);
 *  }
 *
 *  public class GreetingServiceImpl implements GreetingService {
 *    &#64; Override
 *    public Mono<String> sayHello(String name) {
 *      return Mono.just("hello to: " + name);
 *    }
 *  }
 *
 *  // Build a microservices cluster instance:
 *  Microservices microservices = Microservices.builder()
 *       // Introduce GreetingServiceImpl pojo as a micro-service:
 *      .services(new GreetingServiceImpl())
 *      .startAwait();
 *
 *  // Create microservice proxy to GreetingService.class interface:
 *  GreetingService service = microservices.call()
 *      .api(GreetingService.class);
 *
 *  // Invoke the greeting service async:
 *  service.sayHello("joe").subscribe(resp->{
 *    // handle response
 *  });
 *
 * }</pre>
 */
public class Microservices {

  public static final Logger LOGGER = LoggerFactory.getLogger(Microservices.class);

  private final String id;
  private final Metrics metrics;
  private final Map<String, String> tags;
  private final List<ServiceInfo> serviceInfos = new ArrayList<>();
  private final List<ServiceProvider> serviceProviders;
  private final ServiceRegistry serviceRegistry;
  private final ServiceMethodRegistry methodRegistry;
  private final ServiceTransportBootstrap transportBootstrap;
  private final GatewayBootstrap gatewayBootstrap;
  private final ServiceDiscoveryBootstrap discoveryBootstrap;
  private final ServiceProviderErrorMapper errorMapper;

  private Microservices(Builder builder) {
    this.id = UUID.randomUUID().toString();
    this.metrics = builder.metrics;
    this.tags = new HashMap<>(builder.tags);
    this.serviceProviders = new ArrayList<>(builder.serviceProviders);
    this.serviceRegistry = builder.serviceRegistry;
    this.methodRegistry = builder.methodRegistry;
    this.gatewayBootstrap = builder.gatewayBootstrap;
    this.discoveryBootstrap = builder.discoveryBootstrap;
    this.transportBootstrap = builder.transportBootstrap;
    this.errorMapper = builder.errorMapper;
  }

  public static Builder builder() {
    return new Builder();
  }

  public String id() {
    return this.id;
  }

  private Mono<Microservices> start() {
    return transportBootstrap
        .start(methodRegistry)
        .flatMap(
            input -> {
              final ClientTransport clientTransport = transportBootstrap.clientTransport;
              final Address serviceAddress = transportBootstrap.address;
              final Executor workerPool = transportBootstrap.resources.workerPool().orElse(null);

              ServiceCall call = new ServiceCall(clientTransport, methodRegistry, serviceRegistry);

              // invoke service providers and register services
              serviceProviders.stream()
                  .flatMap(serviceProvider -> serviceProvider.provide(call).stream())
                  .forEach(this::collectAndRegister);

              // register services in service registry
              ServiceEndpoint serviceEndpoint = null;
              if (!serviceInfos.isEmpty()) {
                String serviceHost = serviceAddress.host();
                int servicePort = serviceAddress.port();
                serviceEndpoint =
                    ServiceScanner.scan(serviceInfos, id, serviceHost, servicePort, tags);
                serviceRegistry.registerService(serviceEndpoint);
              }

              // configure discovery and publish to the cluster
              return discoveryBootstrap
                  .start(serviceRegistry, serviceEndpoint)
                  .then(Mono.fromCallable(this::doInjection))
                  .then(Mono.defer(() -> startGateway(call, workerPool)))
                  .then(Mono.fromCallable(() -> JmxMonitorMBean.start(this)))
                  .thenReturn(this);
            })
        .onErrorResume(
            ex -> {
              // return original error then shutdown
              return Mono.whenDelayError(Mono.error(ex), shutdown()).cast(Microservices.class);
            });
  }

  private Mono<GatewayBootstrap> startGateway(ServiceCall call, Executor workerPool) {
    return gatewayBootstrap.start(
        new GatewayOptions().workerPool(workerPool).call(call).metrics(metrics));
  }

  private Microservices doInjection() {
    List<Object> serviceInstances =
        serviceInfos.stream().map(ServiceInfo::serviceInstance).collect(Collectors.toList());
    return Reflect.inject(this, serviceInstances);
  }

  private void collectAndRegister(ServiceInfo serviceInfo) {
    // collect
    serviceInfos.add(serviceInfo);

    // register service
    methodRegistry.registerService(
        serviceInfo.serviceInstance(),
        Optional.ofNullable(serviceInfo.errorMapper()).orElse(errorMapper));
  }

  public Metrics metrics() {
    return this.metrics;
  }

  public Address serviceAddress() {
    return transportBootstrap.address;
  }

  public ServiceCall call() {
    return new ServiceCall(transportBootstrap.clientTransport, methodRegistry, serviceRegistry);
  }

  public Gateway gateway(String id) {
    return gatewayBootstrap.gateway(id);
  }

  public ServiceDiscovery discovery() {
    return discoveryBootstrap.discovery;
  }

  /**
   * Shutdown instance and clear resources.
   *
   * @return result of shutdown
   */
  public Mono<Void> shutdown() {
    return Mono.defer(
        () ->
            Mono.whenDelayError(
                discoveryBootstrap.shutdown(),
                gatewayBootstrap.shutdown(),
                transportBootstrap.shutdown()));
  }

  public static final class Builder {

    private Metrics metrics;
    private Map<String, String> tags = new HashMap<>();
    private List<ServiceProvider> serviceProviders = new ArrayList<>();
    private ServiceRegistry serviceRegistry = new ServiceRegistryImpl();
    private ServiceMethodRegistry methodRegistry = new ServiceMethodRegistryImpl();
    private ServiceDiscoveryBootstrap discoveryBootstrap = new ServiceDiscoveryBootstrap();
    private ServiceTransportBootstrap transportBootstrap = new ServiceTransportBootstrap();
    private GatewayBootstrap gatewayBootstrap = new GatewayBootstrap();
    private ServiceProviderErrorMapper errorMapper = DefaultErrorMapper.INSTANCE;

    public Mono<Microservices> start() {
      return Mono.defer(() -> new Microservices(this).start());
    }

    public Microservices startAwait() {
      return start().block();
    }

    public Builder services(ServiceInfo... services) {
      serviceProviders.add(call -> Arrays.stream(services).collect(Collectors.toList()));
      return this;
    }

    /**
     * Adds service instance to microservice.
     *
     * @param services service instance.
     * @return builder
     */
    public Builder services(Object... services) {
      serviceProviders.add(
          call ->
              Arrays.stream(services)
                  .map(
                      s ->
                          s instanceof ServiceInfo
                              ? (ServiceInfo) s
                              : ServiceInfo.fromServiceInstance(s).build())
                  .collect(Collectors.toList()));
      return this;
    }

    public Builder services(ServiceProvider serviceProvider) {
      serviceProviders.add(serviceProvider);
      return this;
    }

    public Builder serviceRegistry(ServiceRegistry serviceRegistry) {
      this.serviceRegistry = serviceRegistry;
      return this;
    }

    public Builder methodRegistry(ServiceMethodRegistry methodRegistry) {
      this.methodRegistry = methodRegistry;
      return this;
    }

    public Builder discovery(Function<ServiceEndpoint, ServiceDiscovery> factory) {
      this.discoveryBootstrap = new ServiceDiscoveryBootstrap(factory);
      return this;
    }

    public Builder transport(UnaryOperator<ServiceTransportBootstrap> op) {
      this.transportBootstrap = op.apply(this.transportBootstrap);
      return this;
    }

    public Builder metrics(MetricRegistry metrics) {
      this.metrics = new Metrics(metrics);
      return this;
    }

    public Builder tags(Map<String, String> tags) {
      this.tags = tags;
      return this;
    }

    public Builder gateway(Function<GatewayOptions, Gateway> factory) {
      gatewayBootstrap.addFactory(factory);
      return this;
    }

    public Builder defaultErrorMapper(ServiceProviderErrorMapper errorMapper) {
      this.errorMapper = errorMapper;
      return this;
    }
  }

  public static class ServiceDiscoveryBootstrap {

    private Function<ServiceEndpoint, ServiceDiscovery> discoveryFactory;

    private ServiceDiscovery discovery;

    private ServiceDiscoveryBootstrap() {}

    private ServiceDiscoveryBootstrap(Function<ServiceEndpoint, ServiceDiscovery> factory) {
      this.discoveryFactory = factory;
    }

    private Mono<ServiceDiscovery> start(
        ServiceRegistry serviceRegistry, ServiceEndpoint serviceEndpoint) {
      return Mono.defer(
          () -> {
            ServiceDiscovery serviceDiscovery = discoveryFactory.apply(serviceEndpoint);
            LOGGER.info("Starting service discovery -- {}", serviceDiscovery);
            return serviceDiscovery
                .start()
                .doOnSuccess(
                    discovery -> {
                      this.discovery = discovery;
                      LOGGER.info("Successfully started service discovery -- {}", this.discovery);
                      listenDiscoveryEvents(serviceRegistry);
                    })
                .doOnError(
                    ex ->
                        LOGGER.error(
                            "Failed to start service discovery -- {}, cause: {}",
                            serviceDiscovery,
                            ex));
          });
    }

    private void listenDiscoveryEvents(ServiceRegistry serviceRegistry) {
      discovery.listen().subscribe(event -> onDiscoveryEvent(serviceRegistry, event));
    }

    private void onDiscoveryEvent(ServiceRegistry serviceRegistry, ServiceDiscoveryEvent event) {
      ServiceEndpoint serviceEndpoint = event.serviceEndpoint();
      if (event.isRegistered()) {
        serviceRegistry.registerService(serviceEndpoint);
      }
      if (event.isUnregistered()) {
        serviceRegistry.unregisterService(serviceEndpoint.id());
      }
    }

    private Mono<Void> shutdown() {
      return Mono.defer(
          () ->
              Optional.ofNullable(discovery) //
                  .map(ServiceDiscovery::shutdown)
                  .orElse(Mono.empty())
                  .doFinally(
                      s -> {
                        if (discovery != null) {
                          LOGGER.info("Service discovery -- {} has been stopped", discovery);
                        }
                      }));
    }
  }

  private static class GatewayBootstrap {

    private final List<Function<GatewayOptions, Gateway>> factories = new ArrayList<>();
    private final List<Gateway> gateways = new CopyOnWriteArrayList<>();

    private GatewayBootstrap addFactory(Function<GatewayOptions, Gateway> factory) {
      this.factories.add(factory);
      return this;
    }

    private Mono<GatewayBootstrap> start(GatewayOptions options) {
      return Flux.fromIterable(factories)
          .flatMap(
              factory -> {
                Gateway gateway = factory.apply(options);
                LOGGER.info("Starting gateway -- {} with {}", gateway, options);
                return gateway
                    .start()
                    .doOnSuccess(gateways::add)
                    .doOnSuccess(
                        result ->
                            LOGGER.info(
                                "Successfully started gateway -- {} on {}",
                                result,
                                result.address()))
                    .doOnError(
                        ex ->
                            LOGGER.error(
                                "Failed to start gateway -- {} with {}, cause: {}",
                                gateway,
                                options,
                                ex));
              })
          .then(Mono.just(this));
    }

    private Mono<Void> shutdown() {
      return Mono.defer(
          () ->
              Mono.whenDelayError(gateways.stream().map(Gateway::stop).toArray(Mono[]::new))
                  .doFinally(
                      s -> {
                        if (!gateways.isEmpty()) {
                          LOGGER.info("Gateways have been stopped");
                        }
                      }));
    }

    private Gateway gateway(String id) {
      return gateways.stream()
          .filter(gw -> gw.id().equals(id))
          .findFirst()
          .orElseThrow(
              () -> new IllegalArgumentException("Didn't find gateway under id: '" + id + "'"));
    }
  }

  public static class ServiceTransportBootstrap {

    private String host = "localhost";
    private int port = 0;
    private Supplier<TransportResources> resourcesSupplier;
    private Function<TransportResources, ClientTransport> clientTransportFactory;
    private Function<TransportResources, ServerTransport> serverTransportFactory;

    private TransportResources resources;
    private ClientTransport clientTransport;
    private ServerTransport serverTransport;
    private Address address;

    private ServiceTransportBootstrap() {}

    private ServiceTransportBootstrap(ServiceTransportBootstrap other) {
      this.host = other.host;
      this.port = other.port;
      this.resourcesSupplier = other.resourcesSupplier;
      this.clientTransportFactory = other.clientTransportFactory;
      this.serverTransportFactory = other.serverTransportFactory;
    }

    private ServiceTransportBootstrap copy() {
      return new ServiceTransportBootstrap(this);
    }

    /**
     * Setting for service host.
     *
     * @param host service host
     * @return new {@code ServiceTransportBootstrap} instance
     */
    public ServiceTransportBootstrap host(String host) {
      ServiceTransportBootstrap c = copy();
      c.host = host;
      return c;
    }

    /**
     * Setting for service port.
     *
     * @param port service port
     * @return new {@code ServiceTransportBootstrap} instance
     */
    public ServiceTransportBootstrap port(int port) {
      ServiceTransportBootstrap c = copy();
      c.port = port;
      return c;
    }

    /**
     * Setting for service transpotr resoruces.
     *
     * @param supplier transport resources provider
     * @return new {@code ServiceTransportBootstrap} instance
     */
    public ServiceTransportBootstrap resources(Supplier<TransportResources> supplier) {
      ServiceTransportBootstrap c = copy();
      c.resourcesSupplier = supplier;
      return c;
    }

    /**
     * Setting for client trnaspotr provider.
     *
     * @param factory client transptr provider
     * @return new {@code ServiceTransportBootstrap} instance
     */
    public ServiceTransportBootstrap client(Function<TransportResources, ClientTransport> factory) {
      ServiceTransportBootstrap c = copy();
      c.clientTransportFactory = factory;
      return c;
    }

    /**
     * Setting for service transport provider.
     *
     * @param factory server transport provider
     * @return new {@code ServiceTransportBootstrap} instance
     */
    public ServiceTransportBootstrap server(Function<TransportResources, ServerTransport> factory) {
      ServiceTransportBootstrap c = copy();
      c.serverTransportFactory = factory;
      return c;
    }

    private Mono<ServiceTransportBootstrap> start(ServiceMethodRegistry methodRegistry) {
      return Mono.fromSupplier(resourcesSupplier)
          .flatMap(TransportResources::start)
          .doOnSuccess(
              resources -> {
                // keep transport resources
                this.resources = resources;
                LOGGER.info(
                    "Successfully started service transport resources -- {}", this.resources);
              })
          .doOnError(ex -> LOGGER.error("Failed to start service transport resources: {}", ex))
          .flatMap(
              resources -> {
                // bind server transport
                ServerTransport serverTransport = serverTransportFactory.apply(resources);
                return serverTransport
                    .bind(port, methodRegistry)
                    .doOnError(
                        ex ->
                            LOGGER.error(
                                "Failed to bind server service "
                                    + "transport -- {} on port: {}, cause: {}",
                                serverTransport,
                                port,
                                ex));
              })
          .doOnSuccess(
              serverTransport -> {
                this.serverTransport = serverTransport;

                // prepare service host:port for exposing
                int port = serverTransport.address().port();
                String host =
                    Optional.ofNullable(this.host)
                        .orElseGet(() -> Address.getLocalIpAddress().getHostAddress());
                this.address = Address.create(host, port);

                LOGGER.info(
                    "Successfully bound server service transport -- {} on address {}",
                    this.serverTransport,
                    this.address);

                // create client transport
                this.clientTransport = clientTransportFactory.apply(resources);
                LOGGER.info(
                    "Successfully created client service transport -- {}", this.clientTransport);
              })
          .thenReturn(this);
    }

    private Mono<Void> shutdown() {
      return Mono.defer(
          () ->
              Mono.whenDelayError(
                      Optional.ofNullable(serverTransport)
                          .map(ServerTransport::stop)
                          .orElse(Mono.empty()),
                      Optional.ofNullable(resources)
                          .map(TransportResources::shutdown)
                          .orElse(Mono.empty()))
                  .doFinally(
                      s -> {
                        if (resources != null) {
                          LOGGER.info("Service transport have been stopped");
                        }
                      }));
    }

    @Override
    public String toString() {
      return "ServiceTransportBootstrap{"
          + "host="
          + host
          + ", port="
          + port
          + ", clientTransport="
          + clientTransport.getClass()
          + ", serverTransport="
          + serverTransport.getClass()
          + ", resources="
          + resources.getClass()
          + "}";
    }
  }

  public interface MonitorMBean {

    Collection<String> getId();

    Collection<String> getDiscoveryAddress();

    Collection<String> getServiceEndpoint();

    Collection<String> getServiceEndpoints();

    Collection<String> getRecentServiceDiscoveryEvents();

    Collection<String> getClientServiceTransport();

    Collection<String> getServerServiceTransport();

    Collection<String> getServiceDiscovery();
  }

  private static class JmxMonitorMBean implements MonitorMBean {

    public static final int MAX_CACHE_SIZE = 128;

    private final Microservices microservices;
    private final ReplayProcessor<ServiceDiscoveryEvent> processor;

    private static JmxMonitorMBean start(Microservices instance) throws Exception {
      MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
      JmxMonitorMBean jmxMBean = new JmxMonitorMBean(instance);
      ObjectName objectName =
          new ObjectName("io.scalecube.services:name=Microservices@" + instance.id);
      StandardMBean standardMBean = new StandardMBean(jmxMBean, MonitorMBean.class);
      mbeanServer.registerMBean(standardMBean, objectName);
      return jmxMBean;
    }

    private JmxMonitorMBean(Microservices microservices) {
      this.microservices = microservices;
      this.processor = ReplayProcessor.create(MAX_CACHE_SIZE);
      microservices.discovery().listen().subscribe(processor);
    }

    @Override
    public Collection<String> getId() {
      return Collections.singletonList(microservices.id());
    }

    @Override
    public Collection<String> getDiscoveryAddress() {
      return Collections.singletonList(microservices.discovery().address().toString());
    }

    @Override
    public Collection<String> getServiceEndpoint() {
      return Collections.singletonList(String.valueOf(microservices.discovery().endpoint()));
    }

    @Override
    public Collection<String> getRecentServiceDiscoveryEvents() {
      List<String> recentEvents = new ArrayList<>(MAX_CACHE_SIZE);
      processor.map(ServiceDiscoveryEvent::toString).subscribe(recentEvents::add);
      return recentEvents;
    }

    @Override
    public Collection<String> getServiceEndpoints() {
      return microservices.serviceRegistry.listServiceEndpoints().stream()
          .map(ServiceEndpoint::toString)
          .collect(Collectors.toList());
    }

    @Override
    public Collection<String> getClientServiceTransport() {
      return Collections.singletonList(microservices.transportBootstrap.clientTransport.toString());
    }

    @Override
    public Collection<String> getServerServiceTransport() {
      return Collections.singletonList(microservices.transportBootstrap.serverTransport.toString());
    }

    @Override
    public Collection<String> getServiceDiscovery() {
      return Collections.singletonList(microservices.discovery().toString());
    }
  }
}
