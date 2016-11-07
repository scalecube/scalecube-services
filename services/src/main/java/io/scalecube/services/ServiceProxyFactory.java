package io.scalecube.services;

import io.scalecube.services.annotations.ServiceProcessor;
import io.scalecube.services.routing.Router;
import io.scalecube.services.routing.RouterFactory;
import io.scalecube.transport.Message;

import com.google.common.reflect.Reflection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ServiceProxyFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceProxyFactory.class);

  private final ServiceProcessor serviceProcessor;
  private Map<String, ServiceDefinition> serviceDefinitions;
  private RouterFactory routerFactory;

  public ServiceProxyFactory(IServiceRegistry serviceRegistry, ServiceProcessor serviceProcessor) {
    this.routerFactory = new RouterFactory(serviceRegistry);
    this.serviceProcessor = serviceProcessor;
  }

  /**
   * createProxy creates a java generic proxy instance by a given service interface.
   * 
   * @param serviceInterface the service interface, api, of the service.
   * @param routerType the type of routing method class to be used.
   * @return newly created service proxy object.
   */
  public <T> T createProxy(Class<T> serviceInterface, final Class<? extends Router> routerType
      , Duration timeout) {

    this.serviceDefinitions = serviceProcessor.introspectServiceInterface(serviceInterface);

    return Reflection.newProxy(serviceInterface, new InvocationHandler() {


      @Override
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

        try {

          ServiceDefinition serviceDefinition = serviceDefinitions.get(method.getName());

          Router router = routerFactory.getRouter(routerType);

          Optional<ServiceInstance> serviceInstance = router.route(serviceDefinition);

          if (serviceInstance.isPresent()) {
            Message reqMsg = Message.withData(args[0])
                .qualifier(serviceInstance.get().serviceName())
                .build();

            CompletableFuture<?> resultFuture =
                (CompletableFuture<?>) serviceInstance.get().invoke(reqMsg, serviceDefinition);

            return timeoutAfter(resultFuture, timeout);

          } else {
            LOGGER.error(
                "Failed  to invoke service, No reachable member with such service definition [{}], args [{}]",
                serviceDefinition, args);
            throw new IllegalStateException("No reachable member with such service: " + method.getName());
          }

        } catch (Throwable ex) {
          LOGGER.error(
              "Failed  to invoke service, No reachable member with such service method [{}], args [{}], error [{}]",
              method, args, ex);
          throw new IllegalStateException("No reachable member with such service: " + method.getName());
        }
      }

      final ScheduledExecutorService delayer = Executors.newScheduledThreadPool(1);

      public CompletableFuture<?> timeoutAfter(final CompletableFuture<?> resultFuture, Duration timeout) {

        final CompletableFuture<Class<Void>> timeoutFuture = new CompletableFuture<>();

        // schedule to terminate the target goal in future in case it was not done yet
        final ScheduledFuture<?> scheduledEvent = delayer.schedule(() -> {
          // by this time the target goal should have finished.
          if (!resultFuture.isDone()) {
            // taget goal not finished in time so cancel it with timeout.
            resultFuture.completeExceptionally(new TimeoutException("expecting response reached timeout!"));
          }
        }, timeout.toMillis(), TimeUnit.MILLISECONDS);

        // cancel the timeout in case target goal did finish on time
        resultFuture.thenRun(() -> {
          if (resultFuture.isDone()) {
            scheduledEvent.cancel(true);
            timeoutFuture.complete(Void.TYPE);
          }
        });

        return resultFuture;
      }
    });
  }
}
