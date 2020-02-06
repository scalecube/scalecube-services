package io.scalecube.services;

import com.codahale.metrics.MetricRegistry;
import io.scalecube.net.Address;
import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.exceptions.DefaultErrorMapper;
import io.scalecube.services.exceptions.ServiceProviderErrorMapper;
import io.scalecube.services.gateway.Gateway;
import io.scalecube.services.gateway.GatewayOptions;
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
public class Microservices {

  public static final Logger LOGGER = LoggerFactory.getLogger(Microservices.class);

  private final String id = generateId();
  private final Metrics metrics;
  private final Map<String, String> tags;
  private final List<ServiceProvider> serviceProviders;
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

  private Microservices(Builder builder) {
    this.metrics = builder.metrics;
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
    LOGGER.info("[{}] Starting", id);

    // Create bootstrap scheduler
    Scheduler scheduler = Schedulers.newSingle(toString(), true);

    return transportBootstrap
        .start(this, methodRegistry)
        .publishOn(scheduler)
        .flatMap(
            input -> {
              final ServiceCall call = call();
              final Address serviceAddress;

              if (input != ServiceTransportBootstrap.noOpInstance) {
                serviceAddress = input.address;
              } else {
                serviceAddress = null;
              }

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
                  .create(serviceEndpointBuilder.build(), serviceRegistry)
                  .publishOn(scheduler)
                  .then(Mono.defer(() -> startGateway(call)).publishOn(scheduler))
                  .then(Mono.fromCallable(() -> Injector.inject(this, serviceInstances)))
                  .then(Mono.fromCallable(() -> JmxMonitorMBean.start(this)))
                  .then(Mono.defer(discoveryBootstrap::start).publishOn(scheduler))
                  .thenReturn(this);
            })
        .onErrorResume(
            ex -> {
              // return original error then shutdown
              return Mono.whenDelayError(Mono.error(ex), shutdown()).cast(Microservices.class);
            })
        .doOnSuccess(m -> listenJvmShutdown())
        .doOnSuccess(m -> LOGGER.info("[{}] Started", id))
        .doOnTerminate(scheduler::dispose);
  }

  private void registerInMethodRegistry(ServiceInfo serviceInfo) {
    methodRegistry.registerService(
        ServiceInfo.from(serviceInfo)
            .errorMapper(Optional.ofNullable(serviceInfo.errorMapper()).orElse(errorMapper))
            .dataDecoder(Optional.ofNullable(serviceInfo.dataDecoder()).orElse(dataDecoder))
            .authenticator(Optional.ofNullable(serviceInfo.authenticator()).orElse(authenticator))
            .build());
  }

  private Mono<GatewayBootstrap> startGateway(ServiceCall call) {
    return gatewayBootstrap.start(this, new GatewayOptions().call(call).metrics(metrics));
  }

  public Metrics metrics() {
    return this.metrics;
  }

  public Address serviceAddress() {
    return transportBootstrap.address;
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

  private Mono<Void> processBeforeDestroy() {
    return Mono.whenDelayError(
        methodRegistry.listServices().stream()
            .map(ServiceInfo::serviceInstance)
            .map(s -> Mono.fromRunnable(() -> Injector.processBeforeDestroy(this, s)))
            .collect(Collectors.toList()));
  }

  public static final class Builder {

    private Metrics metrics;
    private Map<String, String> tags = new HashMap<>();
    private List<ServiceProvider> serviceProviders = new ArrayList<>();
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

    private Function<ServiceEndpoint, ServiceDiscovery> discoveryFactory =
        ignore -> NoOpServiceDiscovery.INSTANCE;

    private ServiceDiscovery discovery;
    private Disposable disposable;

    private ServiceDiscoveryBootstrap() {}

    private ServiceDiscoveryBootstrap(Function<ServiceEndpoint, ServiceDiscovery> factory) {
      this.discoveryFactory = factory;
    }

    /**
     * Creates instance of {@code ServiceDiscovery}.
     *
     * @param serviceEndpoint local service endpoint
     * @param serviceRegistry service registry
     * @return new {@code ServiceDiscovery} instance
     */
    private Mono<ServiceDiscovery> create(
        ServiceEndpoint serviceEndpoint, ServiceRegistry serviceRegistry) {
      return Mono.defer(
          () -> {
            discovery = discoveryFactory.apply(serviceEndpoint);
            disposable =
                discovery
                    .listenDiscovery()
                    .subscribe(
                        discoveryEvent -> {
                          if (discoveryEvent.isEndpointAdded()) {
                            serviceRegistry.registerService(discoveryEvent.serviceEndpoint());
                          }
                          if (discoveryEvent.isEndpointLeaving()
                              || discoveryEvent.isEndpointRemoved()) {
                            serviceRegistry.unregisterService(
                                discoveryEvent.serviceEndpoint().id());
                          }
                        });
            return Mono.just(discovery);
          });
    }

    /**
     * Starts {@code ServiceDiscovery} instance.
     *
     * @return started {@code ServiceDiscovery} instance
     */
    private Mono<ServiceDiscovery> start() {
      return Mono.defer(
          () -> {
            if (discovery == null) {
              throw new IllegalStateException(
                  "Create ServiceDiscovery instance before starting it");
            }
            LOGGER.debug("Starting ServiceDiscovery");
            return discovery
                .start()
                .doOnSuccess(
                    serviceDiscovery -> {
                      discovery = serviceDiscovery;
                      LOGGER.debug("Successfully started ServiceDiscovery -- {}", discovery);
                    })
                .doOnError(
                    ex ->
                        LOGGER.error(
                            "Failed to start ServiceDiscovery -- {}, cause: ", discovery, ex));
          });
    }

    private Mono<Void> shutdown() {
      return Mono.defer(
          () ->
              Optional.ofNullable(discovery) //
                  .map(ServiceDiscovery::shutdown)
                  .orElse(Mono.empty())
                  .doFinally(
                      s -> {
                        if (disposable != null) {
                          disposable.dispose();
                        }
                        if (discovery != null) {
                          LOGGER.debug("ServiceDiscovery -- {} has been stopped", discovery);
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
                                "[{}][{}][{}] Starting",
                                microservices.id(),
                                gateway.getClass().getSimpleName(),
                                gateway.id()))
                    .doOnSuccess(
                        result ->
                            LOGGER.info(
                                "[{}][{}][{}][{}] Started",
                                microservices.id(),
                                result.getClass().getSimpleName(),
                                result.id(),
                                result.address()))
                    .doOnError(
                        ex ->
                            LOGGER.error(
                                "[{}][{}][{}] Failed to start: {}",
                                microservices.id(),
                                gateway.getClass().getSimpleName(),
                                gateway.id(),
                                ex.toString()));
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
                          LOGGER.debug("Gateways have been stopped");
                        }
                      }));
    }

    private List<Gateway> gateways() {
      return new ArrayList<>(gateways);
    }

    private Gateway gateway(String id) {
      return gateways.stream()
          .filter(gw -> gw.id().equals(id))
          .findFirst()
          .orElseThrow(
              () -> new IllegalArgumentException("Didn't find gateway by id: '" + id + "'"));
    }
  }

  public static class ServiceTransportBootstrap {

    public static final ServiceTransportBootstrap noOpInstance = new ServiceTransportBootstrap();

    private final Supplier<ServiceTransport> supplier;

    private ServiceTransport serviceTransport;
    private ClientTransport clientTransport;
    private ServerTransport serverTransport;
    private Address address;

    public ServiceTransportBootstrap() {
      this(null);
    }

    public ServiceTransportBootstrap(Supplier<ServiceTransport> supplier) {
      this.supplier = supplier;
    }

    private Mono<ServiceTransportBootstrap> start(
        Microservices microservices, ServiceMethodRegistry methodRegistry) {

      if (supplier == null) {
        return Mono.just(ServiceTransportBootstrap.noOpInstance);
      }

      serviceTransport = supplier.get();

      return serviceTransport
          .start()
          .doOnSuccess(transport -> serviceTransport = transport)
          .flatMap(transport -> serviceTransport.serverTransport().bind(methodRegistry))
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
              s ->
                  LOGGER.info(
                      "[{}][{}] Starting",
                      microservices.id(),
                      serviceTransport.getClass().getSimpleName()))
          .doOnSuccess(
              transport ->
                  LOGGER.info(
                      "[{}][{}][{}] Started",
                      microservices.id(),
                      serviceTransport.getClass().getSimpleName(),
                      this.address))
          .doOnError(
              ex ->
                  LOGGER.error(
                      "[{}][{}] Failed to start: {}",
                      microservices.id(),
                      serviceTransport.getClass().getSimpleName(),
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
