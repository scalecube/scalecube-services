package io.scalecube.services;

import static com.google.common.base.Preconditions.checkNotNull;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.ICluster;
import io.scalecube.services.annotations.AnnotationServiceProcessor;
import io.scalecube.services.annotations.ServiceProcessor;
import io.scalecube.services.routing.RoundRobinServiceRouter;
import io.scalecube.services.routing.Router;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * The ScaleCube-Services module enables to provision and consuming microservices in a cluster. ScaleCube-Services
 * provides Reactive application development platform for building distributed applications Using microservices and fast
 * data on a message-driven runtime that scales transparently on multi-core, multi-process and/or multi-machines Most
 * microservices frameworks focus on making it easy to build individual microservices. ScaleCube allows developers to
 * run a whole system of microservices from a single command. removing most of the boilerplate code, ScaleCube-Services
 * focuses development on the essence of the service and makes it easy to create explicit and typed protocols that
 * compose. True isolation is achieved through shared-nothing design. This means the services in ScaleCube are
 * autonomous, loosely coupled and mobile (location transparent)—necessary requirements for resilence and elasticity
 * 
 * <p>ScaleCube services requires developers only to two simple Annotations declaring a Service but not regards how you
 * build the service component itself. the Service component is simply java class that implements the service Interface
 * and ScaleCube take care for the rest of the magic. it derived and influenced by Actor model and reactive and
 * streaming patters but does not force application developers to it.
 * 
 * <p>ScaleCube-Services is not yet-anther RPC system in the sense its is cluster aware to provide:
 * <li>location transparency and discovery of service instances.</li>
 * <li>fault tolerance using gossip and failure detection.</li>
 * <li>share nothing - fully distributed and decentralized architecture.</li>
 * <li>Provides fluent, java 8 lambda apis.</li>
 * <li>embeddable and lightweight.</li>
 * <li>utilizes completable futures but primitives and messages can be used as well completable futures gives the
 * advantage of composing and chaining service calls and service results.</li>
 * <li>low latency</li>
 * <li>supports routing extensible strategies when selecting service endpoints</li>
 * 
 * </p><b>basic usage example:</b>
 * 
 * <pre>
 * 
 * <b><font color="green">//Define a serivce interface and implement it.</font></b>
 * {@code
 *    <b>{@literal @}Service</b>
 *    <b><font color="9b0d9b">public interface</font></b> GreetingService {  
 *
 *         <b>{@literal @}ServiceMethod</b>
 *         CompletableFuture<String> asyncGreeting(String string);
 *     }
 *    
 *     <b><font color="9b0d9b">public class</font></b> GreetingServiceImpl implements GreetingService {
 *
 *       {@literal @}Override
 *       <b><font color="9b0d9b">public</font></b> CompletableFuture<String> asyncGreeting(String name) {
 *         <b><font color="9b0d9b">return</font></b> CompletableFuture.completedFuture(" hello to: " + name);
 *       }
 *     }
 *     <b><font color="green">//Build a microservices cluster instance.</font></b>
 *     Microservices microservices = Microservices.builder()
 *       <b><font color="green">//Introduce GreetingServiceImpl pojo as a micro-service.</font></b>
 *         .services(<b><font color="9b0d9b">new</font></b> GreetingServiceImpl())
 *         .build();
 * 
 *     <b><font color="green">//Create microservice proxy to GreetingService.class interface.</font></b>
 *     GreetingService service = microservices.proxy()
 *         .api(GreetingService.class)
 *         .create();
 * 
 *     <b><font color="green">//Invoke the greeting service async.</font></b>
 *     CompletableFuture<String> future = service.asyncGreeting("joe");
 * 
 *     <b><font color="green">//handle completable success or error.</font></b>
 *     future.whenComplete((result, ex) -> {
 *      if (ex == <b><font color="9b0d9b">null</font></b>) {
 *        // print the greeting.
 *         System.<b><font color="9b0d9b">out</font></b>.println(result);
 *       } else {
 *         // print the greeting.
 *         System.<b><font color="9b0d9b">out</font></b>.println(ex);
 *       }
 *     });
 * }
 * </pre>
 */

public class Microservices {

  private static final Logger LOGGER = LoggerFactory.getLogger(Microservices.class);
  private static final ServiceProcessor serviceProcessor = new AnnotationServiceProcessor();

  private final ICluster cluster;

  private final ServiceRegistry serviceRegistry;

  private final ServiceProxyFactory proxyFactory;

  private Microservices(ICluster cluster, ServiceConfig serviceConfig, boolean isSeed) {
    this.cluster = cluster;
    this.serviceRegistry = new ServiceRegistryImpl(cluster, serviceConfig, serviceProcessor, isSeed);
    this.proxyFactory = new ServiceProxyFactory(serviceRegistry, serviceProcessor);
    new ServiceDispatcher(cluster, serviceRegistry);
    this.cluster.listen().subscribe(message -> handleReply(message));
  }


  // Listen response
  private void handleReply(Message message) {
    if (message.header(ServiceHeaders.SERVICE_RESPONSE) != null) {
      String correlationId = message.correlationId();
      Optional<ResponseFuture> optinalFuture = ResponseFuture.get(message.correlationId());
      if (optinalFuture.isPresent()) {
        if (message.header("exception") == null) {
          optinalFuture.get().complete(message);
        } else {
          LOGGER.error("cid [{}] remote service invoke respond with error message {}", correlationId, message);
          optinalFuture.get().completeExceptionally(message.data());
        }
      }
    }
  }

  public ICluster cluster() {
    return this.cluster;
  }

  public void unregisterService(Object serviceObject) {
    serviceRegistry.unregisterService(serviceObject);
  }

  private <T> T createProxy(Class<T> serviceInterface, Class<? extends Router> router, Duration timeout) {
    return proxyFactory.createProxy(serviceInterface, router, timeout);
  }

  public Collection<ServiceInstance> services() {
    return serviceRegistry.services();
  }

  public static final class Builder {

    private Integer port = null;
    private Address[] seeds;
    private Object[] services = new Object[0];
    private ServiceConfig serviceConfig;

    /**
     * microsrrvices instance builder.
     * 
     * @return Microservices instance.
     */
    public Microservices build() {
      if (serviceConfig != null && !serviceConfig.services().isEmpty() && services.length > 0) {
        throw new IllegalStateException(
            "ServiceConfig builder and services() are not allowed to be used in parallel."
                + "please choose ServiceConfig builder in case you wish to register services with service tags");
      }

      if (serviceConfig == null) {
        serviceConfig = ServiceConfig.from(services);
      }
      ClusterConfig cfg = getClusterConfig();
      return new Microservices(Cluster.joinAwait(cfg), serviceConfig, seeds == null);
    }

    private ClusterConfig getClusterConfig() {
      Map<String, String> metadata = new HashMap<>();

      if (!serviceConfig.services().isEmpty()) {
        metadata = Microservices.metadata(serviceConfig);
      }

      ClusterConfig cfg;
      if (port != null && seeds != null) {
        cfg = ConfigAssist.create(port, seeds, metadata);
      } else if (seeds != null) {
        cfg = ConfigAssist.create(seeds, metadata);
      } else if (port != null) {
        cfg = ConfigAssist.create(port, metadata);
      } else {
        cfg = ConfigAssist.create(metadata);
      }
      return cfg;
    }

    public Builder port(int port) {
      this.port = port;
      return this;
    }

    public Builder seeds(Address... seeds) {
      this.seeds = seeds;
      return this;
    }

    /**
     * Services list to be registered.
     * 
     * @param services list of instances decorated with @Service
     * @return builder.
     */
    public Builder services(Object... services) {
      checkNotNull(services);

      if (services != null) {
        this.services = services;
      }
      return this;
    }

    /**
     * Services list to be registered.
     * 
     * @param services list of instances decorated with @Service
     * @return builder.
     */
    public Builder services(ServiceConfig serviceConfig) {
      checkNotNull(serviceConfig);
      this.serviceConfig = serviceConfig;
      return this;
    }
  }

  public static Builder builder() {
    return new Builder();
  }


  public ProxyContext proxy() {
    return new ProxyContext();
  }

  public class ProxyContext {
    private Class<?> api;

    private Class<? extends Router> router = RoundRobinServiceRouter.class;

    private Duration timeout = Duration.ofSeconds(30);

    public <T> T create() {
      LOGGER.debug("create service api {} router {}", this.api, router);
      return (T) createProxy(this.api, this.router, this.timeout);
    }

    public ProxyContext timeout(Duration duration) {
      this.timeout = duration;
      return this;
    }

    public Class<?> api() {
      return api;
    }

    public <T> ProxyContext api(Class<T> api) {
      this.api = api;
      return this;
    }

    public Class<? extends Router> router() {
      return router;
    }

    public ProxyContext router(Class<? extends Router> router) {
      this.router = router;
      return this;
    }
  }

  private static Map<String, String> metadata(ServiceConfig config) {
    Map<String, String> result = new HashMap<>();

    config.services().stream().forEach(service -> {
      Collection<Class<?>> serviceInterfaces = serviceProcessor.extractServiceInterfaces(service.getService());
      for (Class<?> serviceInterface : serviceInterfaces) {
        ServiceDefinition def = serviceProcessor.introspectServiceInterface(serviceInterface);
        result.put(ServiceInfo.toJson(def.serviceName(), service.getTags()), "service");
      }
    });

    return Collections.unmodifiableMap(result);
  }

}
