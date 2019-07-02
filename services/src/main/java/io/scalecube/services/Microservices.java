package io.scalecube.services;

import com.codahale.metrics.MetricRegistry;
import io.scalecube.net.Address;
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
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.DataCodec;
import io.scalecube.services.transport.api.ServerTransport;
import io.scalecube.services.transport.api.ServiceMessageDataDecoder;
import io.scalecube.services.transport.api.ServiceTransport;
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
import reactor.core.publisher.ReplayProcessor;
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

  private final String id;
  private final Metrics metrics;
  private final Map<String, String> tags;
  private final List<ServiceProvider> serviceProviders;
  private final ServiceRegistry serviceRegistry;
  private final ServiceMethodRegistry methodRegistry;
  private final ServiceTransportBootstrap transportBootstrap;
  private final GatewayBootstrap gatewayBootstrap;
  private final ServiceDiscoveryBootstrap discoveryBootstrap;
  private final ServiceProviderErrorMapper errorMapper;
  private final ServiceMessageDataDecoder dataDecoder;
  private final MonoProcessor<Void> shutdown = MonoProcessor.create();
  private final MonoProcessor<Void> onShutdown = MonoProcessor.create();

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
    this.dataDecoder = builder.dataDecoder;

    // Setup cleanup
    shutdown
        .then(doShutdown())
        .doFinally(s -> onShutdown.onComplete())
        .subscribe(null, ex -> LOGGER.warn("Exception occurred on microservices stop: " + ex));
  }

  public static Builder builder() {
    return new Builder();
  }

  public String id() {
    return this.id;
  }

  private Mono<Microservices> start() {
    LOGGER.info("Starting microservices {}", id);

    // Create bootstrap scheduler
    String schedulerName = "microservices" + Integer.toHexString(id.hashCode());
    Scheduler scheduler = Schedulers.newSingle(schedulerName, true);

    return transportBootstrap
        .start(methodRegistry)
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
                  .then(Mono.fromCallable(() -> Reflect.inject(this, serviceInstances)))
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
        .doOnTerminate(scheduler::dispose);
  }

  private void registerInMethodRegistry(ServiceInfo serviceInfo) {
    methodRegistry.registerService(
        serviceInfo.serviceInstance(),
        Optional.ofNullable(serviceInfo.errorMapper()).orElse(errorMapper),
        Optional.ofNullable(serviceInfo.dataDecoder()).orElse(dataDecoder));
  }

  private Mono<GatewayBootstrap> startGateway(ServiceCall call) {
    return gatewayBootstrap.start(new GatewayOptions().call(call).metrics(metrics));
  }

  public Metrics metrics() {
    return this.metrics;
  }

  public Address serviceAddress() {
    return transportBootstrap.address;
  }

  public ServiceCall call() {
    return new ServiceCall(transportBootstrap.clientTransport, serviceRegistry, methodRegistry);
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
    return Mono.defer(
        () -> {
          shutdown.onComplete();
          return onShutdown;
        });
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
          LOGGER.info("Shutting down microservices {}", id);
          return Mono.whenDelayError(
                  discoveryBootstrap.shutdown(),
                  gatewayBootstrap.shutdown(),
                  transportBootstrap.shutdown())
              .doFinally(s -> LOGGER.info("Microservices {} has been shut down", id));
        });
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
                          ServiceEndpoint serviceEndpoint1 = discoveryEvent.serviceEndpoint();
                          if (discoveryEvent.isEndpointAdded()) {
                            serviceRegistry.registerService(serviceEndpoint1);
                          }
                          if (discoveryEvent.isEndpointRemoved()) {
                            serviceRegistry.unregisterService(serviceEndpoint1.id());
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
                  "Create service discovery instance before starting it");
            }
            LOGGER.info("Starting service discovery -- {}", discovery);
            return discovery
                .start()
                .doOnSuccess(
                    serviceDiscovery -> {
                      discovery = serviceDiscovery;
                      LOGGER.info("Successfully started service discovery -- {}", discovery);
                    })
                .doOnError(
                    ex ->
                        LOGGER.error(
                            "Failed to start service discovery -- {}, cause: ", discovery, ex));
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
                                "Failed to start gateway -- {} with {}, cause: ",
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

    private List<Gateway> gateways() {
      return new ArrayList<>(gateways);
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

    private Mono<ServiceTransportBootstrap> start(ServiceMethodRegistry methodRegistry) {
      if (supplier == null) {
        return Mono.just(ServiceTransportBootstrap.noOpInstance);
      }

      serviceTransport = supplier.get();

      return serviceTransport
          .start()
          .doOnSuccess(transport -> serviceTransport = transport) // reset with started
          .flatMap(
              transport -> {
                // bind server transport
                ServerTransport serverTransport = serviceTransport.serverTransport();
                return serverTransport
                    .bind(methodRegistry)
                    .doOnError(
                        ex ->
                            LOGGER.error(
                                "Failed to bind server service " + "transport -- {} cause: ",
                                serverTransport,
                                ex));
              })
          .doOnSuccess(transport -> serverTransport = transport) // reset with bound
          .map(
              transport -> {
                this.address =
                    Address.create(
                        Address.getLocalIpAddress().getHostAddress(),
                        serverTransport.address().port());

                LOGGER.info(
                    "Successfully bound server service transport -- {} on address {}",
                    this.serverTransport,
                    this.address);

                // create client transport
                this.clientTransport = serviceTransport.clientTransport();
                LOGGER.info(
                    "Successfully created client service transport -- {}", this.clientTransport);
                return this;
              });
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

    @Override
    public String toString() {
      return "ServiceTransportBootstrap{"
          + "clientTransport="
          + clientTransport.getClass()
          + ", serverTransport="
          + serverTransport.getClass()
          + "}";
    }
  }

  public interface MonitorMBean {

    Collection<String> getId();

    Collection<String> getDiscoveryAddress();

    Collection<String> getGatewayAddresses();

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
      microservices.discovery().listenDiscovery().subscribe(processor);
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
    public Collection<String> getGatewayAddresses() {
      return microservices.gateways().stream()
          .map(gw -> gw.id() + " -> " + gw.address())
          .collect(Collectors.toList());
    }

    @Override
    public Collection<String> getServiceEndpoint() {
      return Collections.singletonList(String.valueOf(microservices.discovery().serviceEndpoint()));
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
