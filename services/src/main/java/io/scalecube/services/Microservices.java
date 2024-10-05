package io.scalecube.services;

import static reactor.core.publisher.Sinks.EmitFailureHandler.busyLooping;

import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.auth.PrincipalMapper;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import io.scalecube.services.discovery.api.ServiceDiscoveryFactory;
import io.scalecube.services.exceptions.DefaultErrorMapper;
import io.scalecube.services.exceptions.ServiceProviderErrorMapper;
import io.scalecube.services.gateway.Gateway;
import io.scalecube.services.gateway.GatewayOptions;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
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

  // private static final Logger LOGGER = LoggerFactory.getLogger(Microservices.class);

  private final Microservices.Context context;
  private final String id = UUID.randomUUID().toString();

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
    final Microservices microservices = new Microservices(context.conclude());
    try {
      microservices.startTransport(context.transportSupplier, context.serviceRegistry);
      microservices.buildServiceEndpoint();
      microservices.startGateways();
      microservices.createDiscovery();
      microservices.doInject();
      microservices.startListen();
    } catch (Exception ex) {
      microservices.close();
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

  private void buildServiceEndpoint() {
    serviceCall = call();

    final ServiceEndpoint.Builder builder =
        ServiceEndpoint.builder()
            .id(id)
            .address(serviceAddress)
            .contentTypes(DataCodec.getAllContentTypes())
            .tags(context.tags);

    serviceInstances =
        context.serviceProviders.stream()
            .flatMap(serviceProvider -> serviceProvider.provide(serviceCall).stream())
            .peek(this::registerService)
            .peek(
                serviceInfo ->
                    builder.appendServiceRegistrations(ServiceScanner.scanServiceInfo(serviceInfo)))
            .map(ServiceInfo::serviceInstance)
            .collect(Collectors.toList());

    serviceEndpoint = newServiceEndpoint(builder.build());
  }

  private ServiceEndpoint newServiceEndpoint(ServiceEndpoint serviceEndpoint) {
    final ServiceEndpoint.Builder builder = ServiceEndpoint.from(serviceEndpoint);

    final String finalHost =
        Optional.ofNullable(context.externalHost).orElse(serviceEndpoint.address().host());
    final int finalPort =
        Optional.ofNullable(context.externalPort).orElse(serviceEndpoint.address().port());

    return builder.address(Address.create(finalHost, finalPort)).build();
  }

  private void registerService(ServiceInfo serviceInfo) {
    context.serviceRegistry.registerService(
        ServiceInfo.from(serviceInfo)
            .errorMapperIfAbsent(context.defaultErrorMapper)
            .dataDecoderIfAbsent(context.defaultDataDecoder)
            .authenticatorIfAbsent(context.defaultAuthenticator)
            .principalMapperIfAbsent(context.defaultPrincipalMapper)
            .build());
  }

  private void startGateways() {
    final GatewayOptions options = new GatewayOptions().call(serviceCall);
    for (Function<GatewayOptions, Gateway> factory : context.gatewayFactories) {
      gateways.add(factory.apply(options).start());
    }
  }

  private void createDiscovery() {
    scheduler = Schedulers.newSingle("discovery", true);

    final ServiceDiscoveryFactory discoveryFactory = context.discoveryFactory;
    if (discoveryFactory == null) {
      return;
    }

    serviceDiscovery = discoveryFactory.createServiceDiscovery(serviceEndpoint);
  }

  private void doInject() {
    Injector.inject(this, serviceInstances);
  }

  private void startListen() {
    if (serviceDiscovery == null) {
      return;
    }

    // TODO: add onError log

    disposables.add(
        serviceDiscovery
            .listen()
            .subscribeOn(scheduler)
            .publishOn(scheduler)
            .doOnNext(this::onDiscoveryEvent)
            .doOnNext(event -> discoverySink.emitNext(event, busyLooping(Duration.ofSeconds(3))))
            .subscribe());

    serviceDiscovery.start();
    discoveryAddress = serviceDiscovery.address();
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
    invokeBeforeDestroy();
    closeDiscovery();
    closeGateways();
    closeTransport();
  }

  private void invokeBeforeDestroy() {
    context
        .serviceRegistry
        .listServices()
        .forEach(
            serviceInfo -> {
              try {
                Injector.processBeforeDestroy(this, serviceInfo.serviceInstance());
              } catch (Exception ex) {
                // TODO log it
              }
            });
  }

  private void closeTransport() {
    if (clientTransport != null) {
      try {
        clientTransport.close();
      } catch (Exception e) {
        // TODO: log it
      }
    }

    if (serverTransport != null) {
      try {
        serverTransport.stop();
      } catch (Exception e) {
        // TODO: log it
      }
    }

    if (serviceTransport != null) {
      try {
        serviceTransport.stop();
      } catch (Exception e) {
        // TODO: log it
      }
    }
  }

  private void closeGateways() {
    gateways.forEach(
        gateway -> {
          try {
            gateway.stop();
          } catch (Exception ex) {
            // TODO: log it
          }
        });
  }

  private void closeDiscovery() {
    disposables.dispose();

    if (serviceDiscovery != null) {
      try {
        serviceDiscovery.shutdown();
      } catch (Exception ex) {
        // TODO: log it
      }
    }

    if (scheduler != null) {
      scheduler.dispose();
    }
  }

  public static final class Context {

    private final AtomicBoolean isConcluded = new AtomicBoolean();

    private Map<String, String> tags;
    private List<ServiceProvider> serviceProviders;
    private ServiceRegistry serviceRegistry;
    private Authenticator<Object> defaultAuthenticator;
    private PrincipalMapper<Object, Object> defaultPrincipalMapper;
    private ServiceProviderErrorMapper defaultErrorMapper;
    private ServiceMessageDataDecoder defaultDataDecoder;
    private String externalHost;
    private Integer externalPort;
    private ServiceDiscoveryFactory discoveryFactory;
    private Supplier<ServiceTransport> transportSupplier;
    private List<Function<GatewayOptions, Gateway>> gatewayFactories;

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
     * Setter for {@link ServiceRegistry}.
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
     * Setter for supplier of {@link ServiceTransport} instance.
     *
     * @param transportSupplier supplier of {@link ServiceTransport} instance
     * @return this
     */
    public Context transport(Supplier<ServiceTransport> transportSupplier) {
      this.transportSupplier = transportSupplier;
      return this;
    }

    /**
     * Setter for gateway.
     *
     * @param factory gateway factory
     * @return this
     */
    public Context gateway(Function<GatewayOptions, Gateway> factory) {
      gatewayFactories.add(factory);
      return this;
    }

    /**
     * Setter for default {@code errorMapper}. By default, default {@code errorMapper} is set to
     * {@link DefaultErrorMapper#INSTANCE}.
     *
     * @param errorMapper error mapper; not null
     * @return this builder with applied parameter
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
     * @param dataDecoder data decoder; not null
     * @return this builder with applied parameter
     */
    public Context defaultDataDecoder(ServiceMessageDataDecoder dataDecoder) {
      this.defaultDataDecoder = dataDecoder;
      return this;
    }

    /**
     * Setter for default {@code authenticator}. By default, default {@code authenticator} is null.
     *
     * @param authenticator authenticator; optional
     * @return this builder with applied parameter
     */
    public <T> Context defaultAuthenticator(Authenticator<? extends T> authenticator) {
      //noinspection unchecked
      this.defaultAuthenticator = (Authenticator<Object>) authenticator;
      return this;
    }

    /**
     * Setter for default {@code principalMapper}. By default, default {@code principalMapper} is
     * null.
     *
     * @param principalMapper principalMapper; optional
     * @param <T> auth data type
     * @param <R> principal type
     * @return this builder with applied parameter
     */
    public <T, R> Context defaultPrincipalMapper(
        PrincipalMapper<? super T, ? extends R> principalMapper) {
      //noinspection unchecked
      this.defaultPrincipalMapper = (PrincipalMapper<Object, Object>) principalMapper;
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
                .orElse((message, dataType) -> message);
      }

      if (serviceRegistry == null) {
        serviceRegistry = new ServiceRegistryImpl();
      }

      if (tags == null) {
        tags = new HashMap<>();
      }

      if (serviceProviders == null) {
        serviceProviders = new ArrayList<>();
      }

      if (gatewayFactories == null) {
        gatewayFactories = new ArrayList<>();
      }

      return this;
    }
  }
}
