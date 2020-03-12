package io.scalecube.services;

import com.codahale.metrics.MetricRegistry;
import io.scalecube.net.Address;
import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import io.scalecube.services.exceptions.DefaultErrorMapper;
import io.scalecube.services.exceptions.ServiceProviderErrorMapper;
import io.scalecube.services.gateway.Gateway;
import io.scalecube.services.gateway.GatewayOptions;
import io.scalecube.services.inject.ScaleCubeServicesProvider;
import io.scalecube.services.methods.MethodInfo;
import io.scalecube.services.methods.ServiceMethodInvoker;
import io.scalecube.services.methods.ServiceMethodRegistry;
import io.scalecube.services.methods.ServiceMethodRegistryImpl;
import io.scalecube.services.metrics.Metrics;
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
import java.util.Optional;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import sun.misc.Signal;
import sun.misc.SignalHandler;

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
public final class ScaleCube implements Microservices {

  public static final Logger LOGGER = LoggerFactory.getLogger(ScaleCube.class);

  private final String id = generateId();
  private final Metrics metrics;
  private final Map<String, String> tags;
  private final ServicesProvider servicesProvider;
  private final ServiceRegistry serviceRegistry;
  private final ServiceMethodRegistry methodRegistry;
  private final Authenticator<?> authenticator;
  private final ServiceTransportBootstrap transportBootstrap;
  private final GatewayBootstrap gatewayBootstrap;
  private final ServiceDiscoveryBootstrap discoveryBootstrap;
  private final ServiceProviderErrorMapper errorMapper;
  private final ServiceMessageDataDecoder dataDecoder;
  private final MonoProcessor<Void> shutdown = MonoProcessor.create();
  private final MonoProcessor<Void> onShutdown = MonoProcessor.create();

  private ScaleCube(Builder builder) {
    this.metrics = builder.metrics;
    this.tags = new HashMap<>(builder.tags);

    this.servicesProvider = builder.servicesProvider;

    this.serviceRegistry = builder.serviceRegistry;
    this.methodRegistry = builder.methodRegistry;
    this.authenticator = builder.authenticator;
    this.gatewayBootstrap = builder.gatewayBootstrap;
    this.discoveryBootstrap = builder.discoveryBootstrap;
    this.transportBootstrap = builder.transportBootstrap;
    this.errorMapper = builder.errorMapper;
    this.dataDecoder = builder.dataDecoder;

    // Setup cleanup
    shutdown
        .then(doShutdown())
        .doFinally(s -> onShutdown.onComplete())
        .subscribe(
            null, ex -> LOGGER.warn("[{}][doShutdown] Exception occurred: {}", id, ex.toString()));
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public String id() {
    return this.id;
  }

  private static String generateId() {
    return Long.toHexString(UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE);
  }

  @Override
  public String toString() {
    return "Microservices@" + id;
  }

  private Mono<ScaleCube> start() {
    LOGGER.info("[{}][start] Starting", id);

    // Create bootstrap scheduler
    Scheduler scheduler = Schedulers.newSingle(toString(), true);
    Hooks.onOperatorDebug();
    return transportBootstrap
        .start(this)
        // because ServiceTransportBootstrap#address may return nullable value in local case
        .map(transport -> (Supplier<Address>) transport::address)
        .flatMap(this::initializeServiceEndpoint)
        .flatMap(this.discoveryBootstrap::createInstance)
        .publishOn(scheduler)
        .then(this.servicesProvider.provideService(this))
        .doOnNext(this::registerInMethodRegistry)
        .publishOn(scheduler)
        .then(startGateway())
        .publishOn(scheduler)
        .then(Mono.fromCallable(() -> JmxMonitorMBean.start(this)))
        .then(this.discoveryBootstrap.startListen(this))
        .publishOn(scheduler)
        .thenReturn(this)
        .onErrorResume(
            ex -> {
              // return original error then shutdown
              return Mono.whenDelayError(Mono.error(ex), shutdown()).cast(ScaleCube.class);
            })
        .doOnSuccess(m -> listenJvmShutdown())
        .doOnSuccess(m -> LOGGER.info("[{}][start] Started", id))
        .doOnTerminate(scheduler::dispose);
  }

  private Mono<ServiceEndpoint> initializeServiceEndpoint(Supplier<Address> serviceAddress) {
    return this.servicesProvider
        .provideServiceDefinitions(this)
        .map(
            serviceDefinitions -> {
              final ServiceEndpoint.Builder serviceEndpointBuilder =
                  ServiceEndpoint.builder()
                      .id(this.id)
                      .address(serviceAddress.get())
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
    services.forEach(
        serviceInfo -> {
          methodRegistry.registerService(
              ServiceInfo.from(serviceInfo)
                  .errorMapperIfAbsent(this.errorMapper)
                  .dataDecoderIfAbsent(this.dataDecoder)
                  .authenticator(
                      Optional.ofNullable(serviceInfo.authenticator()).orElse(this.authenticator))
                  .build());
        });
  }

  private Mono<GatewayBootstrap> startGateway() {
    return gatewayBootstrap.start(this, new GatewayOptions().call(this.call()).metrics(metrics));
  }

  public Metrics metrics() {
    return this.metrics;
  }

  @Override
  public Address serviceAddress() {
    return transportBootstrap.address;
  }

  /**
   * Creates new instance {@code ServiceCall}.
   *
   * @return new {@code ServiceCall} instance.
   */
  @Override
  public ServiceCall call() {
    return new ServiceCall()
        .transport(transportBootstrap.clientTransport)
        .serviceRegistry(serviceRegistry)
        .methodRegistry(methodRegistry)
        .router(Routers.getRouter(RoundRobinServiceRouter.class));
  }

  public List<Gateway> gateways() {
    return gatewayBootstrap.gateways();
  }

  public Gateway gateway(String id) {
    return gatewayBootstrap.gateway(id);
  }

  @Override
  public ServiceDiscovery discovery() {
    return discoveryBootstrap.discovery;
  }

  /**
   * Shutdown instance and clear resources.
   *
   * @return result of shutdown
   */
  public Mono<Void> shutdown() {
    return Mono.fromRunnable(shutdown::onComplete).then(onShutdown);
  }

  /**
   * Returns signal of when shutdown was completed.
   *
   * @return signal of when shutdown completed
   */
  public Mono<Void> onShutdown() {
    return onShutdown;
  }

  private void listenJvmShutdown() {
    SignalHandler handler = signal -> shutdown.onComplete();
    Signal.handle(new Signal("TERM"), handler);
    Signal.handle(new Signal("INT"), handler);
  }

  private Mono<Void> doShutdown() {
    return Mono.defer(
        () -> {
          LOGGER.info("[{}][doShutdown] Shutting down", id);
          return Mono.whenDelayError(
                  processBeforeDestroy(),
                  discoveryBootstrap.shutdown(),
                  gatewayBootstrap.shutdown(),
                  transportBootstrap.shutdown())
              .doOnSuccess(s -> LOGGER.info("[{}][doShutdown] Shutdown", id));
        });
  }

  private Mono<Microservices> processBeforeDestroy() {
    return servicesProvider.shutDown(this);
  }

  public static final class Builder {

    private Metrics metrics;
    private Map<String, String> tags = new HashMap<>();
    private List<ServiceInfo> services = new ArrayList<>();
    private List<ServiceProvider> serviceProviders = new ArrayList<>();
    private ServicesProvider servicesProvider;
    private ServiceRegistry serviceRegistry = new ServiceRegistryImpl();
    private ServiceMethodRegistry methodRegistry = new ServiceMethodRegistryImpl();
    private Authenticator<?> authenticator = null;
    private ServiceDiscoveryBootstrap discoveryBootstrap = new ServiceDiscoveryBootstrap();
    private ServiceTransportBootstrap transportBootstrap = new ServiceTransportBootstrap();
    private GatewayBootstrap gatewayBootstrap = new GatewayBootstrap();
    private ServiceProviderErrorMapper errorMapper = DefaultErrorMapper.INSTANCE;
    private ServiceMessageDataDecoder dataDecoder =
        Optional.ofNullable(ServiceMessageDataDecoder.INSTANCE)
            .orElse((message, dataType) -> message);

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
      this.servicesProvider =
          this.servicesProvider == null
              ? ScaleCubeServicesProvider.create(this.serviceProviders)
              : this.servicesProvider;
    }

    public Mono<ScaleCube> start() {
      build();
      return Mono.defer(() -> new ScaleCube(this).start());
    }

    public ScaleCube startAwait() {
      return start().block();
    }

    /**
     * Adds service instance to microservice.
     *
     * <p><strong>WARNING</strong> This service will be ignored if custom {@link ServicesProvider}
     * is installed. This method has been left for backward compatibility only and will be removed
     * in future releases.
     *
     * @param services service info instance.
     * @return builder
     * @deprecated use {@link this#serviceProvider(ServicesProvider)}
     */
    @Deprecated
    public Builder services(ServiceInfo... services) {
      this.services.addAll(Arrays.asList(services));
      return this;
    }

    /**
     * Adds service instance to microservice.
     *
     * <p><strong>WARNING</strong> This service will be ignored if custom {@link ServicesProvider}
     * is installed. This method has been left for backward compatibility only and will be removed
     * in future releases.
     *
     * @param services service instance.
     * @return builder
     * @deprecated use {@link this#serviceProvider(ServicesProvider)}
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
     * <p><strong>WARNING</strong> This service will be ignored if custom {@link ServicesProvider}
     * is installed. This method has been left for backward compatibility only and will be removed
     * in future releases.
     *
     * @param serviceProvider - old service provider
     * @return this
     * @deprecated use {@link this#serviceProvider(ServicesProvider)}
     */
    @Deprecated
    public Builder services(ServiceProvider serviceProvider) {
      this.serviceProviders.add(serviceProvider);
      return this;
    }

    public Builder serviceProvider(ServicesProvider serviceProvider) {
      this.servicesProvider = serviceProvider;
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

    public Builder authenticator(Authenticator<?> authenticator) {
      this.authenticator = authenticator;
      return this;
    }

    public Builder discovery(Function<ServiceEndpoint, ServiceDiscovery> factory) {
      this.discoveryBootstrap = new ServiceDiscoveryBootstrap(factory);
      return this;
    }

    public Builder transport(Supplier<ServiceTransport> supplier) {
      this.transportBootstrap = new ServiceTransportBootstrap(supplier);
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

    public Builder defaultDataDecoder(ServiceMessageDataDecoder dataDecoder) {
      this.dataDecoder = dataDecoder;
      return this;
    }
  }

  public static class ServiceDiscoveryBootstrap {

    public static final Function<ServiceEndpoint, ServiceDiscovery> NULL_FACTORY = i -> null;

    private final Function<ServiceEndpoint, ServiceDiscovery> factory;

    private ServiceDiscovery discovery;
    private Disposable disposable;

    private ServiceDiscoveryBootstrap() {
      this(NULL_FACTORY);
    }

    private ServiceDiscoveryBootstrap(Function<ServiceEndpoint, ServiceDiscovery> factory) {
      this.factory = factory;
    }

    private Mono<ServiceDiscovery> createInstance(ServiceEndpoint serviceEndpoint) {
      return factory == NULL_FACTORY
          ? Mono.empty()
          : Mono.defer(() -> Mono.just(discovery = factory.apply(serviceEndpoint)));
    }

    private Mono<ServiceDiscovery> startListen(ScaleCube microservices) {
      return Mono.defer(
          () -> {
            if (this.discovery == null) {
              LOGGER.info("[{}] ServiceDiscovery not set", microservices.id());
              return Mono.empty();
            }

            this.disposable =
                this.discovery
                    .listenDiscovery()
                    .subscribe(event -> onDiscoveryEvent(microservices, event));

            return this.discovery
                .start()
                .doOnSuccess(discovery -> this.discovery = discovery)
                .doOnSubscribe(
                    s -> LOGGER.info("[{}][serviceDiscovery][start] Starting", microservices.id()))
                .doOnSuccess(
                    discovery ->
                        LOGGER.info(
                            "[{}][serviceDiscovery][start] Started, address: {}",
                            microservices.id(),
                            discovery.address()))
                .doOnError(
                    ex ->
                        LOGGER.error(
                            "[{}][serviceDiscovery][start] Exception occurred: {}",
                            microservices.id(),
                            ex.toString()));
          });
    }

    private void onDiscoveryEvent(ScaleCube microservices, ServiceDiscoveryEvent event) {
      if (event.isEndpointAdded()) {
        microservices.serviceRegistry.registerService(event.serviceEndpoint());
      }
      if (event.isEndpointLeaving() || event.isEndpointRemoved()) {
        microservices.serviceRegistry.unregisterService(event.serviceEndpoint().id());
      }
    }

    private Mono<Void> shutdown() {
      return Mono.defer(
          () -> {
            if (disposable != null) {
              disposable.dispose();
            }
            return discovery != null ? discovery.shutdown() : Mono.empty();
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

    private Mono<GatewayBootstrap> start(ScaleCube microservices, GatewayOptions options) {
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

  public static class ServiceTransportBootstrap {

    public static final Supplier<ServiceTransport> NULL_SUPPLIER = () -> null;
    public static final ServiceTransportBootstrap NULL_INSTANCE = new ServiceTransportBootstrap();
    public static final Address NULL_ADDRESS = Address.create("0.0.0.0", -1);

    private final Supplier<ServiceTransport> supplier;

    private ServiceTransport serviceTransport;
    private ClientTransport clientTransport;
    private ServerTransport serverTransport;
    private Address address = NULL_ADDRESS;

    public ServiceTransportBootstrap() {
      this(NULL_SUPPLIER);
    }

    public ServiceTransportBootstrap(Supplier<ServiceTransport> supplier) {
      this.supplier = supplier;
    }

    private Mono<ServiceTransportBootstrap> start(ScaleCube microservices) {
      if (supplier == NULL_SUPPLIER || (serviceTransport = supplier.get()) == null) {
        LOGGER.info("[{}] ServiceTransport not set", microservices.id());
        return Mono.just(NULL_INSTANCE);
      }

      return serviceTransport
          .start()
          .doOnSuccess(transport -> serviceTransport = transport)
          .flatMap(
              transport -> serviceTransport.serverTransport().bind(microservices.methodRegistry))
          .doOnSuccess(transport -> serverTransport = transport)
          .map(
              transport -> {
                this.address =
                    Address.create(
                        Address.getLocalIpAddress().getHostAddress(),
                        serverTransport.address().port());
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
                      this.address))
          .doOnError(
              ex ->
                  LOGGER.error(
                      "[{}][serviceTransport][start] Exception occurred: {}",
                      microservices.id(),
                      ex.toString()));
    }

    public Address address() {
      return address;
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

    private final ScaleCube microservices;

    private static JmxMonitorMBean start(ScaleCube instance) throws Exception {
      MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
      JmxMonitorMBean jmxMBean = new JmxMonitorMBean(instance);
      ObjectName objectName = new ObjectName("io.scalecube.services:name=" + instance.toString());
      StandardMBean standardMBean = new StandardMBean(jmxMBean, MonitorMBean.class);
      mbeanServer.registerMBean(standardMBean, objectName);
      return jmxMBean;
    }

    private JmxMonitorMBean(ScaleCube microservices) {
      this.microservices = microservices;
    }

    @Override
    public String getServiceEndpoint() {
      return String.valueOf(microservices.discovery().serviceEndpoint());
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
                  + invoker.serviceType().getCanonicalName()
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
          .add("auth=" + methodInfo.isAuth())
          .toString();
    }

    private static String asString(ServiceInfo serviceInfo) {
      return new StringJoiner(", ", ServiceMethodInvoker.class.getSimpleName() + "[", "]")
          .add("serviceInstance=" + serviceInfo.serviceInstance())
          .add("tags=" + serviceInfo.tags())
          .add("authenticator=" + serviceInfo.authenticator())
          .toString();
    }
  }
}
