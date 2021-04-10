package io.scalecube.services;

import io.scalecube.net.Address;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.auth.PrincipalMapper;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscoveryContext;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent.Type;
import io.scalecube.services.discovery.api.ServiceDiscoveryFactory;
import io.scalecube.services.discovery.api.ServiceDiscoveryOptions;
import io.scalecube.services.exceptions.DefaultErrorMapper;
import io.scalecube.services.exceptions.ServiceProviderErrorMapper;
import io.scalecube.services.gateway.Gateway;
import io.scalecube.services.gateway.GatewayOptions;
import io.scalecube.services.methods.MethodInfo;
import io.scalecube.services.methods.ServiceMethodInvoker;
import io.scalecube.services.methods.ServiceMethodRegistry;
import io.scalecube.services.methods.ServiceMethodRegistryImpl;
import io.scalecube.services.registry.ServiceRegistryImpl;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.routing.RoundRobinServiceRouter;
import io.scalecube.services.routing.Routers;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.DataCodec;
import io.scalecube.services.transport.api.ServerTransport;
import io.scalecube.services.transport.api.ServiceMessageDataDecoder;
import io.scalecube.services.transport.api.ServiceTransport;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
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
public final class Microservices {

  public static final Logger LOGGER = LoggerFactory.getLogger(Microservices.class);

  private final String id = UUID.randomUUID().toString();
  private final Map<String, String> tags;
  private final List<ServiceProvider> serviceProviders;
  private final ServiceRegistry serviceRegistry;
  private final ServiceMethodRegistry methodRegistry;
  private final Authenticator<Object> defaultAuthenticator;
  private final ServiceTransportBootstrap transportBootstrap;
  private final GatewayBootstrap gatewayBootstrap;
  private final CompositeServiceDiscovery compositeDiscovery;
  private final ServiceProviderErrorMapper defaultErrorMapper;
  private final ServiceMessageDataDecoder defaultDataDecoder;
  private final String defaultContentType;
  private final PrincipalMapper<Object, Object> defaultPrincipalMapper;
  private final Sinks.One<Void> shutdown = Sinks.one();
  private final Sinks.One<Void> onShutdown = Sinks.one();
  private ServiceEndpoint serviceEndpoint;
  private final String externalHost;
  private final Integer externalPort;

  private Microservices(Builder builder) {
    this.tags = Collections.unmodifiableMap(new HashMap<>(builder.tags));
    this.serviceProviders = new ArrayList<>(builder.serviceProviders);
    this.serviceRegistry = builder.serviceRegistry;
    this.methodRegistry = builder.methodRegistry;
    this.defaultAuthenticator = builder.defaultAuthenticator;
    this.gatewayBootstrap = builder.gatewayBootstrap;
    this.compositeDiscovery = builder.compositeDiscovery;
    this.transportBootstrap = builder.transportBootstrap;
    this.defaultErrorMapper = builder.defaultErrorMapper;
    this.defaultDataDecoder = builder.defaultDataDecoder;
    this.defaultContentType = builder.defaultContentType;
    this.defaultPrincipalMapper = builder.defaultPrincipalMapper;
    this.externalHost = builder.externalHost;
    this.externalPort = builder.externalPort;

    // Setup cleanup
    shutdown
        .asMono()
        .then(doShutdown())
        .doFinally(s -> onShutdown.tryEmitEmpty())
        .subscribe(
            null, ex -> LOGGER.warn("[{}][doShutdown] Exception occurred: {}", id, ex.toString()));
  }

  public static Builder builder() {
    return new Builder();
  }

  public String id() {
    return this.id;
  }

  @Override
  public String toString() {
    return "Microservices@" + id;
  }

  private Mono<Microservices> start() {
    LOGGER.info("[{}][start] Starting", id);

    // Create bootstrap scheduler
    Scheduler scheduler = Schedulers.newSingle(toString(), true);

    return transportBootstrap
        .start(this)
        .publishOn(scheduler)
        .flatMap(
            transportBootstrap -> {
              final ServiceCall call = call();
              final Address serviceAddress = transportBootstrap.transportAddress;

              final ServiceEndpoint.Builder serviceEndpointBuilder =
                  ServiceEndpoint.builder()
                      .id(id)
                      .address(serviceAddress)
                      .contentTypes(DataCodec.getAllContentTypes())
                      .tags(tags);

              // invoke service providers and register services
              List<Object> serviceInstances =
                  serviceProviders.stream()
                      .flatMap(serviceProvider -> serviceProvider.provide(call).stream())
                      .peek(this::registerInMethodRegistry)
                      .peek(
                          serviceInfo ->
                              serviceEndpointBuilder.appendServiceRegistrations(
                                  ServiceScanner.scanServiceInfo(serviceInfo)))
                      .map(ServiceInfo::serviceInstance)
                      .collect(Collectors.toList());

              if (transportBootstrap == ServiceTransportBootstrap.NULL_INSTANCE
                  && !serviceInstances.isEmpty()) {
                LOGGER.warn("[{}] ServiceTransport is not set", this.id());
              }

              serviceEndpoint = newServiceEndpoint(serviceEndpointBuilder.build());

              return createDiscovery(
                      this, new ServiceDiscoveryOptions().serviceEndpoint(serviceEndpoint))
                  .publishOn(scheduler)
                  .then(startGateway(new GatewayOptions().call(call)))
                  .publishOn(scheduler)
                  .then(Mono.fromCallable(() -> Injector.inject(this, serviceInstances)))
                  .then(Mono.fromCallable(() -> JmxMonitorMBean.start(this)))
                  .then(compositeDiscovery.startListen())
                  .publishOn(scheduler)
                  .thenReturn(this);
            })
        .onErrorResume(
            ex -> Mono.defer(this::shutdown).then(Mono.error(ex)).cast(Microservices.class))
        .doOnSuccess(m -> LOGGER.info("[{}][start] Started", id))
        .doOnTerminate(scheduler::dispose);
  }

  private ServiceEndpoint newServiceEndpoint(ServiceEndpoint serviceEndpoint) {
    ServiceEndpoint.Builder builder = ServiceEndpoint.from(serviceEndpoint);

    int port = Optional.ofNullable(externalPort).orElse(serviceEndpoint.address().port());

    // calculate local service endpoint address
    Address newAddress =
        Optional.ofNullable(externalHost)
            .map(host -> Address.create(host, port))
            .orElseGet(() -> Address.create(serviceEndpoint.address().host(), port));

    return builder.address(newAddress).build();
  }

  private Mono<GatewayBootstrap> startGateway(GatewayOptions options) {
    return gatewayBootstrap.start(this, options);
  }

  private Mono<ServiceDiscovery> createDiscovery(
      Microservices microservices, ServiceDiscoveryOptions options) {
    return compositeDiscovery.createInstance(microservices, options);
  }

  private void registerInMethodRegistry(ServiceInfo serviceInfo) {
    methodRegistry.registerService(
        ServiceInfo.from(serviceInfo)
            .errorMapperIfAbsent(defaultErrorMapper)
            .dataDecoderIfAbsent(defaultDataDecoder)
            .authenticatorIfAbsent(defaultAuthenticator)
            .principalMapperIfAbsent(defaultPrincipalMapper)
            .build());
  }

  public Address serviceAddress() {
    return transportBootstrap.transportAddress;
  }

  /**
   * Creates new instance {@code ServiceCall}.
   *
   * @return new {@code ServiceCall} instance.
   */
  public ServiceCall call() {
    return new ServiceCall()
        .transport(transportBootstrap.clientTransport)
        .serviceRegistry(serviceRegistry)
        .methodRegistry(methodRegistry)
        .contentType(defaultContentType)
        .errorMapper(DefaultErrorMapper.INSTANCE)
        .router(Routers.getRouter(RoundRobinServiceRouter.class));
  }

  public List<Gateway> gateways() {
    return gatewayBootstrap.gateways();
  }

  public Gateway gateway(String id) {
    return gatewayBootstrap.gateway(id);
  }

  public ServiceEndpoint serviceEndpoint() {
    return serviceEndpoint;
  }

  public List<ServiceEndpoint> serviceEndpoints() {
    return serviceRegistry.listServiceEndpoints();
  }

  /**
   * Returns service discovery context by id.
   *
   * @see Microservices.Builder#discovery(String, ServiceDiscoveryFactory)
   * @param id service discovery id
   * @return service discovery context
   */
  public ServiceDiscoveryContext discovery(String id) {
    return Optional.ofNullable(compositeDiscovery.discoveryContexts.get(id))
        .orElseThrow(() -> new NoSuchElementException("[discovery] id: " + id));
  }

  /**
   * Function to subscribe and listen on the stream of {@code ServiceDiscoveryEvent}\s from
   * composite service discovery instance.
   *
   * <p>Can be called before or after composite service discovery {@code .start()} method call (i.e
   * before of after all service discovery instances will be started). If it's called before then
   * new events will be streamed from all service discovery instances, if it's called after then
   * {@link ServiceRegistry#listServiceEndpoints()} will be turned to service discovery events of
   * type {@link Type#ENDPOINT_ADDED}, and concateneted with a stream of live events.
   *
   * @return stream of {@code ServiceDiscoveryEvent}\s
   */
  public Flux<ServiceDiscoveryEvent> listenDiscovery() {
    return compositeDiscovery.listen();
  }

  /**
   * Shutdown instance and clear resources.
   *
   * @return result of shutdown
   */
  public Mono<Void> shutdown() {
    return Mono.fromRunnable(shutdown::tryEmitEmpty).then(onShutdown.asMono());
  }

  /**
   * Returns signal of when shutdown was completed.
   *
   * @return signal of when shutdown completed
   */
  public Mono<Void> onShutdown() {
    return onShutdown.asMono();
  }

  private Mono<Void> doShutdown() {
    return Mono.defer(
        () -> {
          LOGGER.info("[{}][doShutdown] Shutting down", id);
          return Mono.whenDelayError(
                  processBeforeDestroy(),
                  compositeDiscovery.shutdown(),
                  gatewayBootstrap.shutdown(),
                  transportBootstrap.shutdown())
              .doOnSuccess(s -> LOGGER.info("[{}][doShutdown] Shutdown", id));
        });
  }

  private Mono<Void> processBeforeDestroy() {
    return Mono.whenDelayError(
        methodRegistry.listServices().stream()
            .map(ServiceInfo::serviceInstance)
            .map(s -> Mono.fromRunnable(() -> Injector.processBeforeDestroy(this, s)))
            .collect(Collectors.toList()));
  }

  public static final class Builder {

    private Map<String, String> tags = new HashMap<>();
    private final List<ServiceProvider> serviceProviders = new ArrayList<>();
    private ServiceRegistry serviceRegistry = new ServiceRegistryImpl();
    private ServiceMethodRegistry methodRegistry = new ServiceMethodRegistryImpl();
    private Authenticator<Object> defaultAuthenticator = null;
    private final CompositeServiceDiscovery compositeDiscovery = new CompositeServiceDiscovery();
    private ServiceTransportBootstrap transportBootstrap = new ServiceTransportBootstrap();
    private final GatewayBootstrap gatewayBootstrap = new GatewayBootstrap();
    private ServiceProviderErrorMapper defaultErrorMapper = DefaultErrorMapper.INSTANCE;
    private ServiceMessageDataDecoder defaultDataDecoder =
        Optional.ofNullable(ServiceMessageDataDecoder.INSTANCE)
            .orElse((message, dataType) -> message);
    private String defaultContentType = ServiceMessage.DEFAULT_DATA_FORMAT;
    private PrincipalMapper<Object, Object> defaultPrincipalMapper = null;
    private String externalHost;
    private Integer externalPort;

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

    public Builder externalHost(String externalHost) {
      this.externalHost = externalHost;
      return this;
    }

    public Builder externalPort(Integer externalPort) {
      this.externalPort = externalPort;
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

    public Builder discovery(String id, ServiceDiscoveryFactory discoveryFactory) {
      this.compositeDiscovery.addOperator(opts -> opts.id(id).discoveryFactory(discoveryFactory));
      return this;
    }

    public Builder transport(Supplier<ServiceTransport> supplier) {
      this.transportBootstrap = new ServiceTransportBootstrap(supplier);
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

    /**
     * Setter for default {@code errorMapper}. By default, default {@code errorMapper} is set to
     * {@link DefaultErrorMapper#INSTANCE}.
     *
     * @param errorMapper error mapper; not null
     * @return this builder with applied parameter
     */
    public Builder defaultErrorMapper(ServiceProviderErrorMapper errorMapper) {
      this.defaultErrorMapper = Objects.requireNonNull(errorMapper, "default errorMapper");
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
    public Builder defaultDataDecoder(ServiceMessageDataDecoder dataDecoder) {
      this.defaultDataDecoder = Objects.requireNonNull(dataDecoder, "default dataDecoder");
      return this;
    }

    /**
     * Setter for default {@code contentType}. By default, default {@code contentType} is set to
     * {@link ServiceMessage#DEFAULT_DATA_FORMAT}.
     *
     * @param contentType contentType; not null
     * @return this builder with applied parameter
     */
    public Builder defaultContentType(String contentType) {
      this.defaultContentType = Objects.requireNonNull(contentType, "default contentType");
      return this;
    }

    /**
     * Setter for default {@code authenticator}. By default, default {@code authenticator} is null.
     *
     * @param authenticator authenticator; optional
     * @return this builder with applied parameter
     */
    public <T> Builder defaultAuthenticator(Authenticator<? extends T> authenticator) {
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
    public <T, R> Builder defaultPrincipalMapper(
        PrincipalMapper<? super T, ? extends R> principalMapper) {
      //noinspection unchecked
      this.defaultPrincipalMapper = (PrincipalMapper<Object, Object>) principalMapper;
      return this;
    }
  }

  private static class CompositeServiceDiscovery implements ServiceDiscovery {

    private final List<UnaryOperator<ServiceDiscoveryOptions>> optionOperators = new ArrayList<>();
    private final Map<String, ServiceDiscovery> discoveryInstances = new ConcurrentHashMap<>();
    private final Map<String, ServiceDiscoveryContext> discoveryContexts =
        new ConcurrentHashMap<>();

    // Sink
    private final Sinks.Many<ServiceDiscoveryEvent> sink =
        Sinks.many().multicast().directBestEffort();

    private final Disposable.Composite disposables = Disposables.composite();
    private Scheduler scheduler;
    private Microservices microservices;

    private CompositeServiceDiscovery addOperator(UnaryOperator<ServiceDiscoveryOptions> operator) {
      this.optionOperators.add(operator);
      return this;
    }

    private Mono<ServiceDiscovery> createInstance(
        Microservices microservices, ServiceDiscoveryOptions options) {

      this.microservices = microservices;
      this.scheduler = Schedulers.newSingle("composite-discovery", true);

      for (UnaryOperator<ServiceDiscoveryOptions> operator : this.optionOperators) {

        final ServiceDiscoveryOptions finalOptions = operator.apply(options);
        final String id = finalOptions.id();
        final ServiceEndpoint serviceEndpoint = finalOptions.serviceEndpoint();
        final ServiceDiscovery serviceDiscovery =
            finalOptions.discoveryFactory().createServiceDiscovery(serviceEndpoint);

        discoveryInstances.put(id, serviceDiscovery);

        discoveryContexts.put(
            id,
            ServiceDiscoveryContext.builder()
                .id(id)
                .address(Address.NULL_ADDRESS)
                .discovery(serviceDiscovery)
                .serviceRegistry(microservices.serviceRegistry)
                .scheduler(scheduler)
                .build());
      }

      return Mono.just(this);
    }

    private Mono<Void> startListen() {
      return start() // start composite discovery
          .doOnSubscribe(s -> LOGGER.info("[{}][startListen] Starting", microservices.id()))
          .doOnSuccess(avoid -> LOGGER.info("[{}][startListen] Started", microservices.id()))
          .doOnError(
              ex ->
                  LOGGER.error(
                      "[{}][startListen] Exception occurred: {}",
                      microservices.id(),
                      ex.toString()));
    }

    @Override
    public Flux<ServiceDiscoveryEvent> listen() {
      return Flux.fromStream(microservices.serviceRegistry.listServiceEndpoints().stream())
          .map(ServiceDiscoveryEvent::newEndpointAdded)
          .concatWith(sink.asFlux().onBackpressureBuffer())
          .subscribeOn(scheduler)
          .publishOn(scheduler);
    }

    @Override
    public Mono<Void> start() {
      return Flux.fromIterable(discoveryInstances.entrySet())
          .flatMap(
              entry -> {
                final String id = entry.getKey();
                final ServiceDiscovery discovery = entry.getValue();

                return start0(id, discovery)
                    .doOnSubscribe(s -> LOGGER.info("[discovery][{}][start] Starting", id))
                    .doOnSuccess(avoid -> LOGGER.info("[discovery][{}][start] Started", id))
                    .doOnError(
                        ex ->
                            LOGGER.error(
                                "[discovery][{}][start] Exception occurred: {}",
                                id,
                                ex.toString()));
              })
          .then();
    }

    private Mono<? extends Void> start0(String id, ServiceDiscovery discovery) {
      final ServiceDiscoveryContext.Builder discoveryContextBuilder =
          ServiceDiscoveryContext.from(discoveryContexts.get(id));

      disposables.add(
          discovery
              .listen()
              .subscribeOn(scheduler)
              .publishOn(scheduler)
              .doOnNext(event -> onDiscoveryEvent(microservices, event))
              .doOnNext(sink::tryEmitNext)
              .subscribe());

      return Mono.deferContextual(context -> discovery.start())
          .doOnSuccess(avoid -> discoveryContexts.put(id, discoveryContextBuilder.build()))
          .contextWrite(
              context ->
                  context.put(ServiceDiscoveryContext.Builder.class, discoveryContextBuilder));
    }

    private void onDiscoveryEvent(Microservices microservices, ServiceDiscoveryEvent event) {
      if (event.isEndpointAdded()) {
        microservices.serviceRegistry.registerService(event.serviceEndpoint());
      }
      if (event.isEndpointLeaving() || event.isEndpointRemoved()) {
        microservices.serviceRegistry.unregisterService(event.serviceEndpoint().id());
      }
    }

    @Override
    public Mono<Void> shutdown() {
      return Mono.defer(
          () -> {
            disposables.dispose();
            return Mono.whenDelayError(
                    discoveryInstances.values().stream()
                        .map(ServiceDiscovery::shutdown)
                        .collect(Collectors.toList()))
                .then(Mono.fromRunnable(() -> scheduler.dispose()));
          });
    }
  }

  private static class GatewayBootstrap {

    private final List<Function<GatewayOptions, Gateway>> factories = new ArrayList<>();
    private final List<Gateway> gateways = new CopyOnWriteArrayList<>();

    private GatewayBootstrap addFactory(Function<GatewayOptions, Gateway> factory) {
      this.factories.add(factory);
      return this;
    }

    private Mono<GatewayBootstrap> start(Microservices microservices, GatewayOptions options) {
      return Flux.fromIterable(factories)
          .flatMap(
              factory -> {
                Gateway gateway = factory.apply(options);
                return gateway
                    .start()
                    .doOnSuccess(gateways::add)
                    .doOnSubscribe(
                        s ->
                            LOGGER.info(
                                "[{}][gateway][{}][start] Starting",
                                microservices.id(),
                                gateway.id()))
                    .doOnSuccess(
                        gateway1 ->
                            LOGGER.info(
                                "[{}][gateway][{}][start] Started, address: {}",
                                microservices.id(),
                                gateway1.id(),
                                gateway1.address()))
                    .doOnError(
                        ex ->
                            LOGGER.error(
                                "[{}][gateway][{}][start] Exception occurred: {}",
                                microservices.id(),
                                gateway.id(),
                                ex.toString()));
              })
          .then(Mono.just(this));
    }

    private Mono<Void> shutdown() {
      return Mono.whenDelayError(gateways.stream().map(Gateway::stop).toArray(Mono[]::new));
    }

    private List<Gateway> gateways() {
      return new ArrayList<>(gateways);
    }

    private Gateway gateway(String id) {
      return gateways.stream()
          .filter(gateway -> gateway.id().equals(id))
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException("Didn't find gateway by id=" + id));
    }
  }

  private static class ServiceTransportBootstrap {

    public static final Supplier<ServiceTransport> NULL_SUPPLIER = () -> null;
    public static final ServiceTransportBootstrap NULL_INSTANCE = new ServiceTransportBootstrap();

    private final Supplier<ServiceTransport> transportSupplier;

    private ServiceTransport serviceTransport;
    private ClientTransport clientTransport;
    private ServerTransport serverTransport;
    private Address transportAddress = Address.NULL_ADDRESS;

    public ServiceTransportBootstrap() {
      this(NULL_SUPPLIER);
    }

    public ServiceTransportBootstrap(Supplier<ServiceTransport> transportSupplier) {
      this.transportSupplier = transportSupplier;
    }

    private Mono<ServiceTransportBootstrap> start(Microservices microservices) {
      if (transportSupplier == NULL_SUPPLIER
          || (serviceTransport = transportSupplier.get()) == null) {
        return Mono.just(NULL_INSTANCE);
      }

      return serviceTransport
          .start()
          .doOnSuccess(transport -> serviceTransport = transport) // reset self
          .flatMap(
              transport -> serviceTransport.serverTransport(microservices.methodRegistry).bind())
          .doOnSuccess(transport -> serverTransport = transport)
          .map(
              transport -> {
                this.transportAddress = prepareAddress(serverTransport.address());
                this.clientTransport = serviceTransport.clientTransport();
                return this;
              })
          .doOnSubscribe(
              s -> LOGGER.info("[{}][serviceTransport][start] Starting", microservices.id()))
          .doOnSuccess(
              transport ->
                  LOGGER.info(
                      "[{}][serviceTransport][start] Started, address: {}",
                      microservices.id(),
                      this.serverTransport.address()))
          .doOnError(
              ex ->
                  LOGGER.error(
                      "[{}][serviceTransport][start] Exception occurred: {}",
                      microservices.id(),
                      ex.toString()));
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

    private Mono<Void> shutdown() {
      return Mono.defer(
          () ->
              Flux.concatDelayError(
                      Optional.ofNullable(serverTransport)
                          .map(ServerTransport::stop)
                          .orElse(Mono.empty()),
                      Optional.ofNullable(serviceTransport)
                          .map(ServiceTransport::stop)
                          .orElse(Mono.empty()))
                  .then());
    }
  }

  @SuppressWarnings("unused")
  public interface MonitorMBean {

    String getServiceEndpoint();

    String getAllServiceEndpoints();

    String getServiceMethodInvokers();

    String getServiceInfos();
  }

  private static class JmxMonitorMBean implements MonitorMBean {

    private static final String OBJECT_NAME_FORMAT = "io.scalecube.services:name=%s@%s";

    private final Microservices microservices;

    private static JmxMonitorMBean start(Microservices instance) throws Exception {
      MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
      JmxMonitorMBean jmxMBean = new JmxMonitorMBean(instance);
      ObjectName objectName =
          new ObjectName(String.format(OBJECT_NAME_FORMAT, instance.id(), System.nanoTime()));
      StandardMBean standardMBean = new StandardMBean(jmxMBean, MonitorMBean.class);
      mbeanServer.registerMBean(standardMBean, objectName);
      return jmxMBean;
    }

    private JmxMonitorMBean(Microservices microservices) {
      this.microservices = microservices;
    }

    @Override
    public String getServiceEndpoint() {
      return String.valueOf(microservices.serviceEndpoint);
    }

    @Override
    public String getAllServiceEndpoints() {
      return microservices.serviceRegistry.listServiceEndpoints().stream()
          .map(ServiceEndpoint::toString)
          .collect(Collectors.joining(",", "[", "]"));
    }

    @Override
    public String getServiceMethodInvokers() {
      return microservices.methodRegistry.listInvokers().stream()
          .map(JmxMonitorMBean::asString)
          .collect(Collectors.joining(",", "[", "]"));
    }

    @Override
    public String getServiceInfos() {
      return microservices.methodRegistry.listServices().stream()
          .map(JmxMonitorMBean::asString)
          .collect(Collectors.joining(",", "[", "]"));
    }

    private static String asString(ServiceMethodInvoker invoker) {
      return new StringJoiner(", ", ServiceMethodInvoker.class.getSimpleName() + "[", "]")
          .add("methodInfo=" + asString(invoker.methodInfo()))
          .add(
              "serviceMethod="
                  + invoker.service().getClass().getCanonicalName()
                  + "."
                  + invoker.methodInfo().methodName()
                  + "("
                  + invoker.methodInfo().parameterCount()
                  + ")")
          .toString();
    }

    private static String asString(MethodInfo methodInfo) {
      return new StringJoiner(", ", MethodInfo.class.getSimpleName() + "[", "]")
          .add("qualifier=" + methodInfo.qualifier())
          .add("auth=" + methodInfo.isSecured())
          .toString();
    }

    private static String asString(ServiceInfo serviceInfo) {
      return new StringJoiner(", ", ServiceMethodInvoker.class.getSimpleName() + "[", "]")
          .add("serviceInstance=" + serviceInfo.serviceInstance())
          .add("tags=" + serviceInfo.tags())
          .toString();
    }
  }
}
