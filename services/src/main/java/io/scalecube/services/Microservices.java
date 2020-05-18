package io.scalecube.services;

import io.scalecube.net.Address;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.auth.PrincipalMapper;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
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

  private final String id = generateId();
  private final Map<String, String> tags;
  private final List<ServiceProvider> serviceProviders;
  private final ServiceRegistry serviceRegistry;
  private final ServiceMethodRegistry methodRegistry;
  private final Authenticator<Object> authenticator;
  private final ServiceTransportBootstrap transportBootstrap;
  private final GatewayBootstrap gatewayBootstrap;
  private final ServiceDiscoveryBootstrap discoveryBootstrap;
  private final ServiceProviderErrorMapper errorMapper;
  private final ServiceMessageDataDecoder dataDecoder;
  private final String contentType;
  private final PrincipalMapper<Object, Object> principalMapper;
  private final MonoProcessor<Void> shutdown = MonoProcessor.create();
  private final MonoProcessor<Void> onShutdown = MonoProcessor.create();

  private Microservices(Builder builder) {
    this.tags = new HashMap<>(builder.tags);
    this.serviceProviders = new ArrayList<>(builder.serviceProviders);
    this.serviceRegistry = builder.serviceRegistry;
    this.methodRegistry = builder.methodRegistry;
    this.authenticator = builder.authenticator;
    this.gatewayBootstrap = builder.gatewayBootstrap;
    this.discoveryBootstrap = builder.discoveryBootstrap;
    this.transportBootstrap = builder.transportBootstrap;
    this.errorMapper = builder.errorMapper;
    this.dataDecoder = builder.dataDecoder;
    this.contentType = builder.contentType;
    this.principalMapper = builder.principalMapper;

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

              return discoveryBootstrap
                  .createInstance(serviceEndpointBuilder.build())
                  .publishOn(scheduler)
                  .then(startGateway(call))
                  .publishOn(scheduler)
                  .then(Mono.fromCallable(() -> Injector.inject(this, serviceInstances)))
                  .then(Mono.fromCallable(() -> JmxMonitorMBean.start(this)))
                  .then(discoveryBootstrap.startListen(this))
                  .publishOn(scheduler)
                  .thenReturn(this);
            })
        .onErrorResume(
            ex -> {
              // return original error then shutdown
              return Mono.whenDelayError(Mono.error(ex), shutdown()).cast(Microservices.class);
            })
        .doOnSuccess(m -> LOGGER.info("[{}][start] Started", id))
        .doOnTerminate(scheduler::dispose);
  }

  private void registerInMethodRegistry(ServiceInfo serviceInfo) {
    methodRegistry.registerService(
        ServiceInfo.from(serviceInfo)
            .errorMapperIfAbsent(errorMapper)
            .dataDecoderIfAbsent(dataDecoder)
            .authenticatorIfAbsent(authenticator)
            .principalMapperIfAbsent(principalMapper)
            .build());
  }

  private Mono<GatewayBootstrap> startGateway(ServiceCall call) {
    return gatewayBootstrap.start(this, new GatewayOptions().call(call));
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
        .contentType(contentType)
        .errorMapper(DefaultErrorMapper.INSTANCE)
        .router(Routers.getRouter(RoundRobinServiceRouter.class));
  }

  public List<Gateway> gateways() {
    return gatewayBootstrap.gateways();
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

  private Mono<Void> processBeforeDestroy() {
    return Mono.whenDelayError(
        methodRegistry.listServices().stream()
            .map(ServiceInfo::serviceInstance)
            .map(s -> Mono.fromRunnable(() -> Injector.processBeforeDestroy(this, s)))
            .collect(Collectors.toList()));
  }

  public static final class Builder {

    private Map<String, String> tags = new HashMap<>();
    private List<ServiceProvider> serviceProviders = new ArrayList<>();
    private ServiceRegistry serviceRegistry = new ServiceRegistryImpl();
    private ServiceMethodRegistry methodRegistry = new ServiceMethodRegistryImpl();
    private Authenticator<Object> authenticator = null;
    private ServiceDiscoveryBootstrap discoveryBootstrap = new ServiceDiscoveryBootstrap();
    private ServiceTransportBootstrap transportBootstrap = new ServiceTransportBootstrap();
    private GatewayBootstrap gatewayBootstrap = new GatewayBootstrap();
    private ServiceProviderErrorMapper errorMapper = DefaultErrorMapper.INSTANCE;
    private ServiceMessageDataDecoder dataDecoder =
        Optional.ofNullable(ServiceMessageDataDecoder.INSTANCE)
            .orElse((message, dataType) -> message);
    private String contentType = ServiceMessage.DEFAULT_DATA_FORMAT;
    private PrincipalMapper<Object, Object> principalMapper = authData -> authData;

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

    public Builder discovery(Function<ServiceEndpoint, ServiceDiscovery> factory) {
      this.discoveryBootstrap = new ServiceDiscoveryBootstrap(factory);
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
     * Setter for default {@code errorMapper}.
     *
     * @param errorMapper error mapper
     * @return this builder with applied parameter
     */
    public Builder defaultErrorMapper(ServiceProviderErrorMapper errorMapper) {
      this.errorMapper = errorMapper;
      return this;
    }

    /**
     * Setter for default {@code dataDecoder}.
     *
     * @param dataDecoder data decoder
     * @return this builder with applied parameter
     */
    public Builder defaultDataDecoder(ServiceMessageDataDecoder dataDecoder) {
      this.dataDecoder = dataDecoder;
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
     * Setter for default {@code contentType}.
     *
     * @param contentType contentType; not null
     * @return this builder with applied parameter
     */
    public Builder defaultContentType(String contentType) {
      this.contentType = Objects.requireNonNull(contentType, "default contentType");
      return this;
    }

    /**
     * Setter for default {@code authenticator}.
     *
     * @param authenticator authenticator
     * @return this builder with applied parameter
     */
    @SuppressWarnings("unchecked")
    public <T> Builder defaultAuthenticator(Authenticator<? extends T> authenticator) {
      this.authenticator = (Authenticator<Object>) authenticator;
      return this;
    }

    /**
     * Setter for default {@code principalMapper}.
     *
     * @param principalMapper principalMapper
     * @param <A> auth data type
     * @param <T> principal type
     * @return this builder with applied parameter
     */
    @SuppressWarnings("unchecked")
    public <A, T> Builder defaultPrincipalMapper(
        PrincipalMapper<? extends A, ? extends T> principalMapper) {
      this.principalMapper = (PrincipalMapper<Object, Object>) principalMapper;
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

    private Mono<ServiceDiscovery> startListen(Microservices microservices) {
      return Mono.defer(
          () -> {
            if (discovery == null) {
              LOGGER.info("[{}] ServiceDiscovery not set", microservices.id());
              return Mono.empty();
            }

            disposable =
                discovery
                    .listenDiscovery()
                    .subscribe(event -> onDiscoveryEvent(microservices, event));

            return discovery
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

    private void onDiscoveryEvent(Microservices microservices, ServiceDiscoveryEvent event) {
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

  public static class ServiceTransportBootstrap {

    public static final Supplier<ServiceTransport> NULL_SUPPLIER = () -> null;
    public static final ServiceTransportBootstrap NULL_INSTANCE = new ServiceTransportBootstrap();
    public static final Address NULL_ADDRESS = Address.create("0.0.0.0", 0);

    private final Supplier<ServiceTransport> transportSupplier;

    private ServiceTransport serviceTransport;
    private ClientTransport clientTransport;
    private ServerTransport serverTransport;
    private Address transportAddress = NULL_ADDRESS;

    public ServiceTransportBootstrap() {
      this(NULL_SUPPLIER);
    }

    public ServiceTransportBootstrap(Supplier<ServiceTransport> transportSupplier) {
      this.transportSupplier = transportSupplier;
    }

    private Mono<ServiceTransportBootstrap> start(Microservices microservices) {
      if (transportSupplier == NULL_SUPPLIER
          || (serviceTransport = transportSupplier.get()) == null) {
        LOGGER.info("[{}] ServiceTransport not set", microservices.id());
        return Mono.just(NULL_INSTANCE);
      }

      return serviceTransport
          .start()
          .doOnSuccess(transport -> serviceTransport = transport) // reset self
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
              s -> LOGGER.info("[{}][serviceTransport][start] Starting", microservices.id()))
          .doOnSuccess(
              transport ->
                  LOGGER.info(
                      "[{}][serviceTransport][start] Started, address: {}",
                      microservices.id(),
                      this.transportAddress))
          .doOnError(
              ex ->
                  LOGGER.error(
                      "[{}][serviceTransport][start] Exception occurred: {}",
                      microservices.id(),
                      ex.toString()));
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

    private final Microservices microservices;

    private static JmxMonitorMBean start(Microservices instance) throws Exception {
      MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
      JmxMonitorMBean jmxMBean = new JmxMonitorMBean(instance);
      ObjectName objectName = new ObjectName("io.scalecube.services:name=" + instance.toString());
      StandardMBean standardMBean = new StandardMBean(jmxMBean, MonitorMBean.class);
      mbeanServer.registerMBean(standardMBean, objectName);
      return jmxMBean;
    }

    private JmxMonitorMBean(Microservices microservices) {
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
