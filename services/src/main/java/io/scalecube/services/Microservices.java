package io.scalecube.services;

import static reactor.core.publisher.Sinks.EmitFailureHandler.busyLooping;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.auth.PrincipalMapper;
import io.scalecube.services.auth.ServiceRolesProcessor;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import io.scalecube.services.discovery.api.ServiceDiscoveryFactory;
import io.scalecube.services.exceptions.DefaultErrorMapper;
import io.scalecube.services.exceptions.ServiceProviderErrorMapper;
import io.scalecube.services.gateway.Gateway;
import io.scalecube.services.registry.ServiceRegistryImpl;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.routing.RoundRobinServiceRouter;
import io.scalecube.services.routing.Routers;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.DataCodec;
import io.scalecube.services.transport.api.ServerTransport;
import io.scalecube.services.transport.api.ServiceMessageDataDecoder;
import io.scalecube.services.transport.api.ServiceTransport;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

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
public class Microservices implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Microservices.class);

  private final Microservices.Context context;
  private final UUID id = UUID.randomUUID();
  private final String instanceId = Integer.toHexString(id.hashCode());

  private ServiceTransport serviceTransport;
  private ClientTransport clientTransport;
  private ServerTransport serverTransport;
  private Address serviceAddress = Address.NULL_ADDRESS;
  private ServiceEndpoint serviceEndpoint;
  private ServiceCall serviceCall;
  private List<Object> serviceInstances;
  private final List<Gateway> gateways = new ArrayList<>();
  private ServiceDiscovery serviceDiscovery;
  private final Sinks.Many<ServiceDiscoveryEvent> discoverySink =
      Sinks.many().multicast().directBestEffort();
  private final Disposable.Composite disposables = Disposables.composite();
  private Scheduler scheduler;
  private Address discoveryAddress;

  private Microservices(Microservices.Context context) {
    this.context = context;
  }

  public static Microservices start(Microservices.Context context) {
    Microservices microservices = null;
    try {
      microservices = new Microservices(context.conclude());
      LOGGER.info("[{}] Starting {}", microservices.instanceId, microservices);
      microservices.startTransport(context.transportSupplier, context.serviceRegistry);
      microservices.createServiceEndpoint();
      microservices.startGateways();
      microservices.createDiscovery();
      microservices.doInject();
      microservices.processServiceRoles();
      microservices.startListen();
      LOGGER.info("[{}] Started {}", microservices.instanceId, microservices);
    } catch (Exception ex) {
      if (microservices != null) {
        microservices.close();
      }
      context.close();
      throw Exceptions.propagate(ex);
    }
    return microservices;
  }

  private void startTransport(
      Supplier<ServiceTransport> transportSupplier, ServiceRegistry serviceRegistry) {
    if (transportSupplier == null) {
      return;
    }

    serviceTransport = transportSupplier.get().start();
    serverTransport = serviceTransport.serverTransport(serviceRegistry).bind();
    clientTransport = serviceTransport.clientTransport();
    serviceAddress = prepareAddress(serverTransport.address());

    LOGGER.info(
        "[{}] Started {}, serviceAddress: {}", instanceId, serviceTransport, serviceAddress);
  }

  private static Address prepareAddress(Address address) {
    final InetAddress inetAddress;
    try {
      inetAddress = InetAddress.getByName(address.host());
    } catch (UnknownHostException e) {
      throw Exceptions.propagate(e);
    }
    if (inetAddress.isAnyLocalAddress()) {
      return Address.create(Address.getLocalIpAddress().getHostAddress(), address.port());
    } else {
      return Address.create(inetAddress.getHostAddress(), address.port());
    }
  }

  private void createServiceEndpoint() {
    serviceCall = call();

    final ServiceEndpoint.Builder builder =
        ServiceEndpoint.builder()
            .id(id.toString())
            .name(context.name)
            .address(serviceAddress)
            .contentTypes(DataCodec.getAllContentTypes())
            .tags(context.tags);

    serviceInstances =
        context.serviceProviders.stream()
            .flatMap(serviceProvider -> serviceProvider.provide(serviceCall).stream())
            .peek(this::registerService)
            .peek(s -> builder.appendServiceRegistrations(ServiceScanner.toServiceRegistrations(s)))
            .map(ServiceInfo::serviceInstance)
            .toList();

    serviceEndpoint = enhanceServiceEndpoint(builder.build());

    LOGGER.info(
        "[{}] Created serviceEndpoint: {}, serviceInstances: {}",
        instanceId,
        serviceEndpoint,
        serviceInstances);
  }

  private ServiceEndpoint enhanceServiceEndpoint(ServiceEndpoint serviceEndpoint) {
    final var address = serviceEndpoint.address();
    return ServiceEndpoint.from(serviceEndpoint)
        .address(
            Address.create(
                Optional.ofNullable(context.externalHost).orElse(address.host()),
                Optional.ofNullable(context.externalPort).orElse(address.port())))
        .serviceRegistrations(
            ServiceScanner.replacePlaceholders(serviceEndpoint.serviceRegistrations(), this))
        .build();
  }

  private void registerService(ServiceInfo serviceInfo) {
    context.serviceRegistry.registerService(
        ServiceInfo.from(serviceInfo)
            .errorMapperIfAbsent(context.defaultErrorMapper)
            .dataDecoderIfAbsent(context.defaultDataDecoder)
            .principalMapperIfAbsent(context.defaultPrincipalMapper)
            .loggerIfAbsent(context.defaultLogger)
            .build(),
        context.schedulers,
        qualifier -> ServiceScanner.replacePlaceholders(qualifier, this));
  }

  private void startGateways() {
    for (var factory : context.gatewaySuppliers) {
      final var gateway = factory.get().start(serviceCall, context.serviceRegistry);
      gateways.add(gateway);
      LOGGER.info(
          "[{}] Started {}, gateway: {}@{}", instanceId, gateway, gateway.id(), gateway.address());
    }
  }

  private void createDiscovery() {
    scheduler = Schedulers.newSingle("discovery", true);

    final ServiceDiscoveryFactory discoveryFactory = context.discoveryFactory;
    if (discoveryFactory == null) {
      return;
    }

    serviceDiscovery = discoveryFactory.createServiceDiscovery(serviceEndpoint);

    LOGGER.info("[{}] Created {}", instanceId, serviceDiscovery);
  }

  private void doInject() {
    Injector.inject(this, serviceInstances);
  }

  private void processServiceRoles() {
    final var serviceRolesProcessor = context.serviceRolesProcessor;
    if (serviceRolesProcessor != null) {
      serviceRolesProcessor.process(ServiceScanner.collectServiceRoles(serviceInstances));
    }
  }

  private void startListen() {
    if (serviceDiscovery == null) {
      return;
    }

    disposables.add(
        serviceDiscovery
            .listen()
            .subscribeOn(scheduler)
            .publishOn(scheduler)
            .doOnNext(this::onDiscoveryEvent)
            .doOnNext(event -> discoverySink.emitNext(event, busyLooping(Duration.ofSeconds(3))))
            .doOnError(ex -> LOGGER.error("[{}] Exception occurred", instanceId, ex))
            .subscribe());

    serviceDiscovery.start();
    discoveryAddress = serviceDiscovery.address();

    LOGGER.info(
        "[{}] Started {}, discoveryAddress: {}", instanceId, serviceDiscovery, discoveryAddress);
  }

  private void onDiscoveryEvent(ServiceDiscoveryEvent event) {
    final ServiceRegistry serviceRegistry = context.serviceRegistry;

    if (event.isEndpointAdded()) {
      serviceRegistry.registerService(event.serviceEndpoint());
    }

    if (event.isEndpointLeaving() || event.isEndpointRemoved()) {
      serviceRegistry.unregisterService(event.serviceEndpoint().id());
    }
  }

  /**
   * Returns listening address of the {@link ServerTransport}.
   *
   * @return address, or null (if {@link ServiceTransport} was not specified)
   */
  public Address serviceAddress() {
    return serviceAddress;
  }

  /**
   * Returns new instance of {@link ServiceCall}.
   *
   * @return new instance of {@link ServiceCall}
   */
  public ServiceCall call() {
    return new ServiceCall()
        .transport(clientTransport)
        .serviceRegistry(context.serviceRegistry)
        .router(Routers.getRouter(RoundRobinServiceRouter.class));
  }

  /**
   * Returns started gateway instances. Returned list can be empty, if {@link #gateway(String)} was
   * not called.
   *
   * @return list {@link Gateway} objects, or empty list, if gateways were not specified
   */
  public List<Gateway> gateways() {
    return gateways;
  }

  /**
   * Returns gateway by id.
   *
   * @param id gateway id
   * @return {@link Gateway} instance, or throwing exception if gateway cannot be found
   */
  public Gateway gateway(String id) {
    return gateways.stream()
        .filter(gateway -> gateway.id().equals(id))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Cannot find gateway by id=" + id));
  }

  /**
   * Returns local {@link ServiceEndpoint#id()}.
   *
   * @return local {@link ServiceEndpoint#id()}
   */
  public String id() {
    return id.toString();
  }

  /**
   * Returns local {@link ServiceEndpoint} object.
   *
   * @return local {@link ServiceEndpoint} object
   */
  public ServiceEndpoint serviceEndpoint() {
    return serviceEndpoint;
  }

  /**
   * Returns list of {@link ServiceEndpoint} objects. Service endpoints being landed into {@link
   * Microservices} instance through the {@link ServiceRegistry#registerService(ServiceEndpoint)},
   * which by turn is called by listening and handling service discovery events.
   *
   * @return service endpoints
   */
  public List<ServiceEndpoint> serviceEndpoints() {
    return context.serviceRegistry.listServiceEndpoints();
  }

  /**
   * Returns service tags.
   *
   * @return service tags.
   */
  public Map<String, String> tags() {
    return context.tags;
  }

  /**
   * Returns {@link ServiceRegistry}.
   *
   * @return service registry
   */
  public ServiceRegistry serviceRegistry() {
    return context.serviceRegistry;
  }

  /**
   * Returns listening address of the {@link ServiceDiscovery}.
   *
   * @return address, or null (if {@link ServiceDiscovery} was not specified)
   */
  public Address discoveryAddress() {
    return discoveryAddress;
  }

  /**
   * Function to subscribe and listen on the stream of {@link ServiceDiscoveryEvent} objects from
   * {@link ServiceDiscovery} instance.
   *
   * @return stream of {@link ServiceDiscoveryEvent} objects
   */
  public Flux<ServiceDiscoveryEvent> listenDiscovery() {
    return Flux.fromStream(context.serviceRegistry.listServiceEndpoints().stream())
        .map(ServiceDiscoveryEvent::newEndpointAdded)
        .concatWith(discoverySink.asFlux().onBackpressureBuffer())
        .subscribeOn(scheduler)
        .publishOn(scheduler);
  }

  @Override
  public void close() {
    LOGGER.info("[{}] Closing {} ...", instanceId, this);
    processBeforeDestroy();
    closeDiscovery();
    closeGateways();
    closeTransport();
    context.close();
    LOGGER.info("[{}] Closed {}", instanceId, this);
  }

  private void processBeforeDestroy() {
    context
        .serviceRegistry
        .listServices()
        .forEach(
            serviceInfo -> {
              try {
                Injector.processBeforeDestroy(this, serviceInfo.serviceInstance());
              } catch (Exception e) {
                LOGGER.error(
                    "[{}][processBeforeDestroy] Exception occurred: {}", instanceId, e.toString());
              }
            });
  }

  private void closeTransport() {
    if (clientTransport != null) {
      try {
        clientTransport.close();
      } catch (Exception e) {
        LOGGER.error(
            "[{}][clientTransport.close] Exception occurred: {}", instanceId, e.toString());
      }
    }

    if (serverTransport != null) {
      try {
        serverTransport.stop();
      } catch (Exception e) {
        LOGGER.error(
            "[{}][serverTransport.close] Exception occurred: {}", instanceId, e.toString());
      }
    }

    if (serviceTransport != null) {
      try {
        serviceTransport.stop();
      } catch (Exception e) {
        LOGGER.error(
            "[{}][serviceTransport.stop] Exception occurred: {}", instanceId, e.toString());
      }
    }
  }

  private void closeGateways() {
    gateways.forEach(
        gateway -> {
          try {
            gateway.stop();
          } catch (Exception e) {
            LOGGER.error("[{}][gateway.stop] Exception occurred: {}", instanceId, e.toString());
          }
        });
  }

  private void closeDiscovery() {
    disposables.dispose();

    if (serviceDiscovery != null) {
      try {
        serviceDiscovery.shutdown();
      } catch (Exception e) {
        LOGGER.error("[{}][closeDiscovery] Exception occurred: {}", instanceId, e.toString());
      }
    }

    if (scheduler != null) {
      scheduler.dispose();
    }
  }

  public static final class Context {

    private final AtomicBoolean isConcluded = new AtomicBoolean();

    private String name;
    private Map<String, String> tags;
    private final List<ServiceProvider> serviceProviders = new ArrayList<>();
    private ServiceRegistry serviceRegistry;
    private PrincipalMapper defaultPrincipalMapper;
    private ServiceProviderErrorMapper defaultErrorMapper;
    private ServiceMessageDataDecoder defaultDataDecoder;
    private Logger defaultLogger;
    private String externalHost;
    private Integer externalPort;
    private ServiceDiscoveryFactory discoveryFactory;
    private Supplier<ServiceTransport> transportSupplier;
    private final List<Supplier<Gateway>> gatewaySuppliers = new ArrayList<>();
    private final Map<String, Scheduler> schedulers = new ConcurrentHashMap<>();
    private ServiceRolesProcessor serviceRolesProcessor;

    public Context() {}

    /**
     * Setter for services.
     *
     * @param services {@link ServiceInfo} objects
     * @return this
     */
    public Context services(ServiceInfo... services) {
      serviceProviders.add(call -> Arrays.stream(services).collect(Collectors.toList()));
      return this;
    }

    /**
     * Setter for services. All services that are not instance of {@link ServiceInfo} will be
     * wrapped into {@link ServiceInfo}.
     *
     * @param services services
     * @return this
     */
    public Context services(Object... services) {
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

    /**
     * Setter for {@link ServiceProvider}.
     *
     * @param serviceProvider serviceProvider
     * @return this
     */
    public Context services(ServiceProvider serviceProvider) {
      serviceProviders.add(serviceProvider);
      return this;
    }

    /**
     * Setter for externalHost. If specified, this host will be assgined to the host of the {@link
     * ServiceEndpoint#address()}.
     *
     * @param externalHost externalHost
     * @return this
     */
    public Context externalHost(String externalHost) {
      this.externalHost = externalHost;
      return this;
    }

    /**
     * Setter for externalPort. If specified, this port will be assgined to the port of the {@link
     * ServiceEndpoint#address()}.
     *
     * @param externalPort externalPort
     * @return this
     */
    public Context externalPort(Integer externalPort) {
      this.externalPort = externalPort;
      return this;
    }

    /**
     * Setter for logical service name.
     *
     * @param name logical service name.
     * @return this
     */
    public Context name(String name) {
      this.name = name;
      return this;
    }

    /**
     * Setter for tags.
     *
     * @param tags tags
     * @return this
     */
    public Context tags(Map<String, String> tags) {
      this.tags = tags;
      return this;
    }

    /**
     * Setter for custom {@link ServiceRegistry} instance.
     *
     * @param serviceRegistry serviceRegistry
     * @return this
     */
    public Context serviceRegistry(ServiceRegistry serviceRegistry) {
      this.serviceRegistry = serviceRegistry;
      return this;
    }

    /**
     * Setter for {@link ServiceDiscoveryFactory}.
     *
     * @param discoveryFactory discoveryFactory
     * @return this
     */
    public Context discovery(ServiceDiscoveryFactory discoveryFactory) {
      this.discoveryFactory = discoveryFactory;
      return this;
    }

    /**
     * Setter for {@link ServiceTransport} supplier.
     *
     * @param transportSupplier {@link ServiceTransport} supplier
     * @return this
     */
    public Context transport(Supplier<ServiceTransport> transportSupplier) {
      this.transportSupplier = transportSupplier;
      return this;
    }

    /**
     * Adds {@link Gateway} supplier to the list of gateway suppliers.
     *
     * @param gatewaySupplier {@link Gateway} supplier
     * @return this
     */
    public Context gateway(Supplier<Gateway> gatewaySupplier) {
      gatewaySuppliers.add(gatewaySupplier);
      return this;
    }

    /**
     * Setter for default {@code errorMapper}. By default, default {@code errorMapper} is set to
     * {@link DefaultErrorMapper#INSTANCE}.
     *
     * @param errorMapper error mapper
     * @return this
     */
    public Context defaultErrorMapper(ServiceProviderErrorMapper errorMapper) {
      this.defaultErrorMapper = errorMapper;
      return this;
    }

    /**
     * Setter for default {@code dataDecoder}. By default, default {@code dataDecoder} is set to
     * {@link ServiceMessageDataDecoder#INSTANCE} if it exists, otherswise to a function {@code
     * (message, dataType) -> message}
     *
     * @param dataDecoder data decoder
     * @return this
     */
    public Context defaultDataDecoder(ServiceMessageDataDecoder dataDecoder) {
      this.defaultDataDecoder = dataDecoder;
      return this;
    }

    /**
     * Setter for default {@code principalMapper}. By default, default {@code principalMapper} is
     * null.
     *
     * @param principalMapper principalMapper (optional)
     * @return this
     */
    public Context defaultPrincipalMapper(PrincipalMapper principalMapper) {
      this.defaultPrincipalMapper = principalMapper;
      return this;
    }

    /**
     * Setter for default {@code logger}.
     *
     * @param name logger name (optional)
     * @return this
     */
    public Context defaultLogger(String name) {
      this.defaultLogger = name != null ? LoggerFactory.getLogger(name) : null;
      return this;
    }

    /**
     * Setter for default {@code logger}.
     *
     * @param clazz logger name (optional)
     * @return this
     */
    public Context defaultLogger(Class<?> clazz) {
      this.defaultLogger = clazz != null ? LoggerFactory.getLogger(clazz) : null;
      return this;
    }

    /**
     * Setter for default {@code logger}.
     *
     * @param logger logger (optional)
     * @return this
     */
    public Context defaultLogger(Logger logger) {
      this.defaultLogger = logger;
      return this;
    }

    /**
     * Adds {@link Scheduler} supplier to the list of scheduler suppliers.
     *
     * @param name scheduler name
     * @param supplier {@link Scheduler} supplier
     * @return this
     */
    public Context scheduler(String name, Supplier<Scheduler> supplier) {
      schedulers.put(name, supplier.get());
      return this;
    }

    /**
     * Setter for {@link ServiceRolesProcessor}.
     *
     * @param serviceRolesProcessor serviceRolesProcessor
     * @return this
     */
    public Context serviceRolesProcessor(ServiceRolesProcessor serviceRolesProcessor) {
      this.serviceRolesProcessor = serviceRolesProcessor;
      return this;
    }

    private Context conclude() {
      if (!isConcluded.compareAndSet(false, true)) {
        throw new IllegalStateException("Context is already concluded");
      }

      if (defaultErrorMapper == null) {
        defaultErrorMapper = DefaultErrorMapper.INSTANCE;
      }

      if (defaultDataDecoder == null) {
        defaultDataDecoder =
            Optional.ofNullable(ServiceMessageDataDecoder.INSTANCE)
                .orElse(
                    new ServiceMessageDataDecoder() {
                      @Override
                      public ServiceMessage decodeData(ServiceMessage message, Class<?> dataType) {
                        return message;
                      }

                      @Override
                      public ServiceMessage copyData(ServiceMessage message, Class<?> dataType) {
                        return message;
                      }
                    });
      }

      if (name == null) {
        name = Long.toHexString(Long.MAX_VALUE & UUID.randomUUID().getMostSignificantBits());
      }

      if (tags == null) {
        tags = new HashMap<>();
      }

      if (serviceRegistry == null) {
        serviceRegistry = new ServiceRegistryImpl();
      }

      schedulers.put("parallel", Schedulers.parallel());
      schedulers.put("single", Schedulers.single());
      schedulers.put("boundedElastic", Schedulers.boundedElastic());
      schedulers.put("immediate", Schedulers.immediate());

      return this;
    }

    private void close() {
      schedulers.values().forEach(Scheduler::dispose);
      schedulers.clear();
    }
  }
}
