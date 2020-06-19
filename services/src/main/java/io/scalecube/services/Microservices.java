package io.scalecube.services;

import io.scalecube.net.Address;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.auth.DelegatingAuthenticator;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import java.util.stream.Stream;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.Context;

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

  private final String id = UUID.randomUUID().toString().replace("-", "");
  private final Map<String, String> tags;
  private final ServiceFactory serviceFactory;
  private final ServiceRegistry serviceRegistry;
  private final ServiceMethodRegistry methodRegistry;
  private final Authenticator<Object> defaultAuthenticator;
  private final ServiceTransportBootstrap transportBootstrap;
  private final GatewayBootstrap gatewayBootstrap;
  private final ServiceProviderErrorMapper defaultErrorMapper;
  private final ServiceMessageDataDecoder defaultDataDecoder;
  private final CompositeServiceDiscovery compositeDiscovery;
  private final String contentType;
  private final PrincipalMapper<Object, Object> principalMapper;
  private final MonoProcessor<Void> shutdown = MonoProcessor.create();
  private final MonoProcessor<Void> onShutdown = MonoProcessor.create();

  private Microservices(Builder builder) {
    this.tags = new HashMap<>(builder.tags);

    this.serviceRegistry = builder.serviceRegistry;
    this.methodRegistry = builder.methodRegistry;
    this.defaultAuthenticator = builder.authenticator;
    this.gatewayBootstrap = builder.gatewayBootstrap;
    this.compositeDiscovery = builder.compositeDiscovery;
    this.transportBootstrap = builder.transportBootstrap;
    this.defaultErrorMapper = builder.errorMapper;
    this.defaultDataDecoder = builder.dataDecoder;
    this.contentType = builder.contentType;
    this.principalMapper = builder.principalMapper;

    this.serviceFactory =
        builder.serviceFactory != null
            ? builder.serviceFactory
            : ScalecubeServiceFactory.create(builder.serviceProviders);
    // for initialization services by deprecated ServiceProvider
    if (this.serviceFactory instanceof ScalecubeServiceFactory) {
      ((ScalecubeServiceFactory) this.serviceFactory).setServiceCall(this::serviceCall);
    }

    // Setup cleanup
    this.shutdown
        .then(doShutdown())
        .doFinally(s -> this.onShutdown.onComplete())
        .subscribe(
            null, ex -> LOGGER.warn("[{}][doShutdown] Exception occurred: {}", id, ex.toString()));
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Unique id of this Microservices node.
   *
   * @return id
   */
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
    return this.transportBootstrap
        .start(this)
        .map(ServiceTransportBootstrap::address)
        .flatMap(this::initializeServiceEndpoint)
        .flatMap(this::createDiscovery)
        .map(serviceDiscovery -> new MicroservicesContextImpl(serviceDiscovery, this::serviceCall))
        .flatMap(this.serviceFactory::initializeServices)
        .doOnNext(this::registerInMethodRegistry)
        .publishOn(scheduler)
        .then(startGateway())
        .publishOn(scheduler)
        .then(Mono.fromCallable(() -> JmxMonitorMBean.start(this)))
        .then(this.compositeDiscovery.startListen(this))
        .publishOn(scheduler)
        .thenReturn(this)
        .onErrorResume(
            ex -> Mono.defer(this::shutdown).then(Mono.error(ex)).cast(Microservices.class))
        .doOnSuccess(m -> LOGGER.info("[{}][start] Started", id))
        .doOnTerminate(scheduler::dispose);
  }

  private Mono<CompositeServiceDiscovery> createDiscovery(ServiceEndpoint serviceEndpoint) {
    return this.compositeDiscovery.createInstance(serviceEndpoint);
  }

  private Mono<ServiceEndpoint> initializeServiceEndpoint(Address serviceAddress) {
    Mono<? extends Collection<ServiceDefinition>> serviceDefinitionsMono =
        Mono.fromCallable(this.serviceFactory::getServiceDefinitions);
    return serviceDefinitionsMono.map(
        serviceDefinitions -> {
          final ServiceEndpoint.Builder serviceEndpointBuilder =
              ServiceEndpoint.builder()
                  .id(this.id)
                  .address(serviceAddress)
                  .contentTypes(DataCodec.getAllContentTypes())
                  .tags(this.tags);
          serviceDefinitions.forEach(
              serviceDefinition ->
                  serviceEndpointBuilder.appendServiceRegistrations(
                      ServiceScanner.scanServiceDefinition(serviceDefinition)));
          return serviceEndpointBuilder.build();
        });
  }

  private void registerInMethodRegistry(Collection<ServiceInfo> services) {
    services.stream()
        .map(
            serviceInfo ->
                ServiceInfo.from(serviceInfo)
                    .errorMapperIfAbsent(this.defaultErrorMapper)
                    .dataDecoderIfAbsent(this.defaultDataDecoder)
                    .authenticatorIfAbsent(this.defaultAuthenticator)
                    .principalMapperIfAbsent(this.principalMapper)
                    .build())
        .forEach(this.methodRegistry::registerService);
  }

  private Mono<GatewayBootstrap> startGateway() {
    return this.gatewayBootstrap.start(this, new GatewayOptions().call(this.serviceCall()));
  }

  /**
   * Network address of this Microservices node.
   *
   * @return network address
   */
  public Address serviceAddress() {
    return transportBootstrap.transportAddress;
  }

  /**
   * Creates new instance {@code ServiceCall}.
   *
   * @return new {@code ServiceCall} instance.
   * @deprecated use {@link this#serviceCall()}
   */
  @Deprecated
  public ServiceCall call() {
    return this.serviceCall();
  }

  /**
   * Creates new instance {@code ServiceCall}.
   *
   * @return new {@code ServiceCall} instance.
   */
  public ServiceCall serviceCall() {
    return new ServiceCall()
        .transport(this.transportBootstrap.clientTransport)
        .serviceRegistry(this.serviceRegistry)
        .methodRegistry(this.methodRegistry)
        .contentType(this.contentType)
        .errorMapper(DefaultErrorMapper.INSTANCE)
        .router(Routers.getRouter(RoundRobinServiceRouter.class));
  }

  /**
   * List of all gateway registered in this Microservices node.
   *
   * @return gateways
   */
  public List<Gateway> gateways() {
    return this.gatewayBootstrap.gateways();
  }

  /**
   * Get gateway registered in this Microservices node by id.
   *
   * @return gateway
   */
  public Gateway gateway(String id) {
    return this.gatewayBootstrap.gateway(id);
  }

  /**
   * Service endpoint.
   *
   * @return service endpoint
   */
  public ServiceEndpoint serviceEndpoint() {
    return this.compositeDiscovery.serviceEndpoint;
  }

  /**
   * Returns service discovery context by id.
   *
   * @param id service discovery id
   * @return service discovery context
   */
  public ServiceDiscoveryContext discovery(String id) {
    return Optional.ofNullable(this.compositeDiscovery.discoveryContexts.get(id))
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
    return this.compositeDiscovery.listen();
  }

  /**
   * Shutdown instance and clear resources.
   *
   * @return result of shutdown
   */
  public Mono<Void> shutdown() {
    return Mono.fromRunnable(this.shutdown::onComplete).then(this.onShutdown);
  }

  /**
   * Returns signal of when shutdown was completed.
   *
   * @return signal of when shutdown completed
   */
  public Mono<Void> onShutdown() {
    return this.onShutdown;
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
    return this.serviceFactory.shutdownServices().then();
  }

  private static class CompositeServiceDiscovery implements ServiceDiscovery {

    private final List<UnaryOperator<ServiceDiscoveryOptions>> operatorList = new ArrayList<>();
    private final Map<String, ServiceDiscovery> discoveryMap = new HashMap<>();
    private final Map<String, ServiceDiscoveryContext> discoveryContexts = new ConcurrentHashMap<>();

    // Subject
    private final DirectProcessor<ServiceDiscoveryEvent> subject = DirectProcessor.create();
    private final FluxSink<ServiceDiscoveryEvent> sink = subject.sink();

    private final Disposable.Composite disposables = Disposables.composite();
    private Scheduler scheduler;
    private ServiceEndpoint serviceEndpoint;

    private CompositeServiceDiscovery addOperator(UnaryOperator<ServiceDiscoveryOptions> operator) {
      this.operatorList.add(operator);
      return this;
    }

    private Mono<CompositeServiceDiscovery> createInstance(ServiceEndpoint serviceEndpoint) {
      this.serviceEndpoint = serviceEndpoint;
      ServiceDiscoveryOptions options =
          new ServiceDiscoveryOptions().serviceEndpoint(this.serviceEndpoint);
      for (UnaryOperator<ServiceDiscoveryOptions> operator : operatorList) {
        final ServiceDiscoveryOptions finalOptions = operator.apply(options);
        final String id = finalOptions.id();
        this.discoveryMap.put(
            id, finalOptions.discoveryFactory().createServiceDiscovery(serviceEndpoint));
      }

      this.scheduler = Schedulers.newSingle("composite-discovery", true);

      return Mono.just(this);
    }

    private Mono<Void> startListen(Microservices microservices) {
      return Mono.deferWithContext(context -> start())
          .doOnSubscribe(s -> LOGGER.info("[{}][startListen] Starting", microservices.id))
          .doOnSuccess(discovery -> LOGGER.info("[{}][startListen] Started", microservices.id))
          .doOnError(
              ex ->
                  LOGGER.error(
                      "[{}][startListen] Exception occurred: {}", microservices.id, ex.toString()))
          .subscriberContext(
              context -> reactor.util.context.Context.of(Microservices.class, microservices));
    }

    @Override
    public Flux<ServiceDiscoveryEvent> listen() {
      return subject.onBackpressureBuffer();
    }

    @Override
    public Mono<Void> start() {
      return Flux.fromIterable(discoveryMap.entrySet())
          .flatMap(
              entry -> {
                final String id = entry.getKey();
                final ServiceDiscovery discovery = entry.getValue();

                return Mono.deferWithContext(context -> start0(discovery, context))
                    .doOnSubscribe(s -> LOGGER.info("[discovery][{}][start] Starting", id))
                    .doOnSuccess(avoid -> LOGGER.info("[discovery][{}][start] Started", id))
                    .doOnError(
                        ex ->
                            LOGGER.error(
                                "[discovery][{}][start] Exception occurred: {}", id, ex.toString()))
                    .subscriberContext(
                        context ->
                            context.put(
                                ServiceDiscoveryContext.Builder.class,
                                ServiceDiscoveryContext.builder().id(id).discovery(discovery)));
              })
          .then();
    }

    private Mono<? extends Void> start0(
        ServiceDiscovery discovery, Context innerMicroservicesContext) {
      final Microservices microservices = innerMicroservicesContext.get(Microservices.class);
      ServiceDiscoveryContext.Builder builder =
          innerMicroservicesContext.get(ServiceDiscoveryContext.Builder.class);

      disposables.add(
          discovery
              .listen()
              .publishOn(scheduler)
              .doOnNext(event -> onDiscoveryEvent(microservices, event))
              .doOnNext(sink::next)
              .subscribe());

      return discovery
          .start()
          .doOnSuccess(
              avoid -> {
                ServiceDiscoveryContext discoveryContext = builder.build();
                discoveryContexts.put(discoveryContext.id(), discoveryContext);
              });
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
                discoveryMap.values().stream()
                    .map(ServiceDiscovery::shutdown)
                    .collect(Collectors.toList()))
                .then(Mono.fromRunnable(() -> scheduler.dispose()));
          });
    }
  }

  public static final class Builder {

    private Map<String, String> tags = new HashMap<>();
    private final List<ServiceInfo> services = new ArrayList<>();
    private final List<ServiceProvider> serviceProviders = new ArrayList<>();
    private ServiceFactory serviceFactory;
    private ServiceRegistry serviceRegistry = new ServiceRegistryImpl();
    private ServiceMethodRegistry methodRegistry = new ServiceMethodRegistryImpl();
    private Authenticator<Object> authenticator = new DelegatingAuthenticator();
    private final CompositeServiceDiscovery compositeDiscovery = new CompositeServiceDiscovery();
    private ServiceTransportBootstrap transportBootstrap = new ServiceTransportBootstrap();
    private final GatewayBootstrap gatewayBootstrap = new GatewayBootstrap();
    private ServiceProviderErrorMapper errorMapper = DefaultErrorMapper.INSTANCE;
    private ServiceMessageDataDecoder dataDecoder =
        Optional.ofNullable(ServiceMessageDataDecoder.INSTANCE)
            .orElse((message, dataType) -> message);
    private String contentType = ServiceMessage.DEFAULT_DATA_FORMAT;
    private PrincipalMapper<Object, Object> principalMapper = authData -> authData;

    private void build() {
      ServiceProvider serviceProvider =
          this.services.stream()
              .map(
                  service ->
                      ServiceInfo.from(service)
                          .dataDecoderIfAbsent(this.dataDecoder)
                          .errorMapperIfAbsent(this.errorMapper)
                          .build())
              .collect(
                  Collectors.collectingAndThen(
                      Collectors.toList(), services -> (ServiceProvider) call -> services));
      this.serviceProviders.add(serviceProvider);
    }

    public Mono<Microservices> start() {
      build();
      return Mono.defer(() -> new Microservices(this).start());
    }

    public Microservices startAwait() {
      return start().block();
    }

    /**
     * Adds service instance to microservice.
     *
     * <p><strong>WARNING</strong> This service will be ignored if custom {@link ServiceFactory} is
     * installed. This method has been left for backward compatibility only and will be removed in
     * future releases.
     *
     * @param services service info instance.
     * @return builder
     * @deprecated use {@link this#serviceFactory(ServiceFactory)}
     */
    @Deprecated
    public Builder services(ServiceInfo... services) {
      this.services.addAll(Arrays.asList(services));
      return this;
    }

    /**
     * Adds service instance to microservice.
     *
     * <p><strong>WARNING</strong> This service will be ignored if custom {@link ServiceFactory} is
     * installed. This method has been left for backward compatibility only and will be removed in
     * future releases.
     *
     * @param services service instance.
     * @return builder
     * @deprecated use {@link this#serviceFactory(ServiceFactory)}
     */
    @Deprecated
    public Builder services(Object... services) {
      Stream.of(services)
          .map(service -> ServiceInfo.fromServiceInstance(service).build())
          .forEach(this.services::add);
      return this;
    }

    /**
     * Set up service provider.
     *
     * <p><strong>WARNING</strong> This service will be ignored if custom {@link ServiceFactory} is
     * installed. This method has been left for backward compatibility only and will be removed in
     * future releases.
     *
     * @param serviceProvider - old service provider
     * @return this
     * @deprecated use {@link this#serviceFactory(ServiceFactory)}
     */
    @Deprecated
    public Builder services(ServiceProvider serviceProvider) {
      this.serviceProviders.add(serviceProvider);
      return this;
    }

    public Builder serviceFactory(ServiceFactory serviceFactory) {
      this.serviceFactory = serviceFactory;
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

    /**
     * Setter for default {@code authenticator}. Deprecated. Use {@link
     * #defaultAuthenticator(Authenticator)}.
     *
     * @param authenticator authenticator
     * @return this builder with applied parameter
     */
    @Deprecated
    public <T> Builder authenticator(Authenticator<? extends T> authenticator) {
      return defaultAuthenticator(authenticator);
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
      this.errorMapper = Objects.requireNonNull(errorMapper, "default errorMapper");
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
      this.dataDecoder = Objects.requireNonNull(dataDecoder, "default dataDecoder");
      return this;
    }

    /**
     * Setter for default {@code contentType}. Deprecated. Use {@link #defaultContentType(String)}.
     *
     * @param contentType contentType; not null
     * @return this builder with applied parameter
     */
    @Deprecated
    public Builder contentType(String contentType) {
      return defaultContentType(contentType);
    }

    /**
     * Setter for default {@code contentType}. By default, default {@code contentType} is set to
     * {@link ServiceMessage#DEFAULT_DATA_FORMAT}.
     *
     * @param contentType contentType; not null
     * @return this builder with applied parameter
     */
    public Builder defaultContentType(String contentType) {
      this.contentType = Objects.requireNonNull(contentType, "default contentType");
      return this;
    }

    /**
     * Setter for default {@code authenticator}. By default, default {@code authenticator} is set to
     * {@link DelegatingAuthenticator}.
     *
     * @param authenticator authenticator; not null
     * @return this builder with applied parameter
     */
    @SuppressWarnings("unchecked")
    public <T> Builder defaultAuthenticator(Authenticator<? extends T> authenticator) {
      Objects.requireNonNull(authenticator, "default authenticator");
      this.authenticator = (Authenticator<Object>) authenticator;
      return this;
    }

    /**
     * Setter for default {@code principalMapper}. By default, default {@code principalMapper} is
     * set to unary function {@code authData -> authData}.
     *
     * @param principalMapper principalMapper; not null
     * @param <A> auth data type
     * @param <T> principal type
     * @return this builder with applied parameter
     */
    @SuppressWarnings("unchecked")
    public <A, T> Builder defaultPrincipalMapper(
        PrincipalMapper<? extends A, ? extends T> principalMapper) {
      Objects.requireNonNull(principalMapper, "default principalMapper");
      this.principalMapper = (PrincipalMapper<Object, Object>) principalMapper;
      return this;
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
                                microservices.id,
                                gateway.id()))
                    .doOnSuccess(
                        gateway1 ->
                            LOGGER.info(
                                "[{}][gateway][{}][start] Started, address: {}",
                                microservices.id,
                                gateway1.id(),
                                gateway1.address()))
                    .doOnError(
                        ex ->
                            LOGGER.error(
                                "[{}][gateway][{}][start] Exception occurred: {}",
                                microservices.id,
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
      if (this.transportSupplier == NULL_SUPPLIER
          || (this.serviceTransport = this.transportSupplier.get()) == null) {
        return Mono.just(NULL_INSTANCE);
      }

      return this.serviceTransport
          .start()
          .doOnSuccess(transport -> this.serviceTransport = transport) // reset self
          .flatMap(
              transport -> serviceTransport.serverTransport().bind(microservices.methodRegistry))
          .doOnSuccess(transport -> serverTransport = transport)
          .map(
              transport -> {
                this.transportAddress =
                    Address.create(
                        Address.getLocalIpAddress().getHostAddress(),
                        serverTransport.address().port());
                this.clientTransport = serviceTransport.clientTransport();
                return this;
              })
          .doOnSubscribe(
              s -> LOGGER.info("[{}][serviceTransport][start] Starting", microservices.id))
          .doOnSuccess(
              transport ->
                  LOGGER.info(
                      "[{}][serviceTransport][start] Started, address: {}",
                      microservices.id,
                      this.transportAddress))
          .doOnError(
              ex ->
                  LOGGER.error(
                      "[{}][serviceTransport][start] Exception occurred: {}",
                      microservices.id,
                      ex.toString()));
    }

    public Address address() {
      return this.transportAddress;
    }

    private Mono<Void> shutdown() {
      return Mono.defer(
          () ->
              Flux.concatDelayError(
                  Optional.ofNullable(this.serverTransport)
                      .map(ServerTransport::stop)
                      .orElse(Mono.empty()),
                  Optional.ofNullable(this.serviceTransport)
                      .map(ServiceTransport::stop)
                      .orElse(Mono.empty()))
                  .then());
    }
  }

  private static class JmxMonitorMBean implements MonitorMBean {

    private static final String OBJECT_NAME_FORMAT = "io.scalecube.services:name=%s@%s";

    private final Microservices microservices;

    private static JmxMonitorMBean start(Microservices instance) throws Exception {
      MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
      JmxMonitorMBean jmxMBean = new JmxMonitorMBean(instance);
      ObjectName objectName =
          new ObjectName(
              String.format(OBJECT_NAME_FORMAT, instance.id(), System.nanoTime()));
      StandardMBean standardMBean = new StandardMBean(jmxMBean, MonitorMBean.class);
      mbeanServer.registerMBean(standardMBean, objectName);
      return jmxMBean;
    }

    private JmxMonitorMBean(Microservices microservices) {
      this.microservices = microservices;
    }

    @Override
    public String getServiceEndpoint() {
      return String.valueOf(this.microservices);
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
                  + invoker.service()
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

  @SuppressWarnings("unused")
  public interface MonitorMBean {

    String getServiceEndpoint();

    String getAllServiceEndpoints();

    String getServiceMethodInvokers();

    String getServiceInfos();
  }

  private static final class MicroservicesContextImpl implements MicroservicesContext {

    private final CompositeServiceDiscovery serviceDiscovery;
    private final Supplier<ServiceCall> serviceCallSupplier;

    private MicroservicesContextImpl(
        CompositeServiceDiscovery serviceDiscovery, Supplier<ServiceCall> serviceCallSupplier) {
      this.serviceDiscovery = serviceDiscovery;
      this.serviceCallSupplier = serviceCallSupplier;
    }

    @Override
    public ServiceEndpoint serviceEndpoint() {
      return this.serviceDiscovery.serviceEndpoint;
    }

    @Override
    public ServiceCall serviceCall() {
      return this.serviceCallSupplier.get();
    }

    @Override
    public Flux<ServiceDiscoveryEvent> listenDiscovery() {
      return this.serviceDiscovery.listen();
    }
  }
}
