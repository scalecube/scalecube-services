package io.scalecube.services;

import io.scalecube.services.annotations.ServiceProcessor;
import io.scalecube.services.routing.Router;
import io.scalecube.services.routing.RouterFactory;
import io.scalecube.transport.Message;

import com.google.common.reflect.Reflection;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ServiceProxyFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceProxyFactory.class);

  /**
   * used to complete the request future with timeout exception in case no response comes from service.
   */
  private static final ScheduledExecutorService delayer = Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder().setNameFormat("sc-services-timeout").setDaemon(true).build());

  private final ServiceProcessor serviceProcessor;
  ServiceDefinition serviceDefinition;
  private RouterFactory routerFactory;

  public ServiceProxyFactory(ServiceRegistry serviceRegistry, ServiceProcessor serviceProcessor) {
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
  public <T> T createProxy(Class<T> serviceInterface, final Class<? extends Router> routerType,
      Duration timeout) {

    this.serviceDefinition = serviceProcessor.introspectServiceInterface(serviceInterface);

    return Reflection.newProxy(serviceInterface, new InvocationHandler() {

      @Override
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        try {
          // fetch the service definition by the method name
          Router router = routerFactory.getRouter(routerType);

          Optional<ServiceInstance> optionalServiceInstance = router.route(serviceDefinition);

          if (optionalServiceInstance.isPresent()) {
            ServiceInstance serviceInstance = optionalServiceInstance.get();
            Object data = method.getParameterCount() != 0 ? args[0] : null;
            Message reqMsg = Message.withData(data)
                .header(ServiceHeaders.SERVICE_REQUEST, serviceInstance.serviceName())
                .header(ServiceHeaders.METHOD, method.getName())
                .build();

            CompletableFuture<?> resultFuture =
                (CompletableFuture<?>) serviceInstance.invoke(reqMsg, serviceDefinition);

            if (method.getReturnType().equals(Void.TYPE)) {
              return CompletableFuture.completedFuture(Void.TYPE);
            } else {
              return timeoutAfter(resultFuture, timeout);
            }
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

      public CompletableFuture<?> timeoutAfter(final CompletableFuture<?> resultFuture, Duration timeout) {

        final CompletableFuture<Class<Void>> timeoutFuture = new CompletableFuture<>();

        // schedule to terminate the target goal in future in case it was not done yet
        final ScheduledFuture<?> scheduledEvent = delayer.schedule(() -> {
          // by this time the target goal should have finished.
          if (!resultFuture.isDone()) {
            // target goal not finished in time so cancel it with timeout.
            resultFuture.completeExceptionally(new TimeoutException("expecting response reached timeout!"));
          }
        }, timeout.toMillis(), TimeUnit.MILLISECONDS);

        // cancel the timeout in case target goal did finish on time
        resultFuture.thenRun(() -> {
          if (resultFuture.isDone()) {
            if (!scheduledEvent.isDone()) {
              scheduledEvent.cancel(false);
            }
            timeoutFuture.complete(Void.TYPE);
          }
        });
        return resultFuture;
      }
    });
  }
}
