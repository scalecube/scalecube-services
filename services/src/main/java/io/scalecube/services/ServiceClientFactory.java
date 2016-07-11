package io.scalecube.services;

import com.google.common.reflect.Reflection;
import com.google.common.util.concurrent.Futures;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Collection;

import io.scalecube.cluster.ClusterMember;
import io.scalecube.cluster.ClusterMemberStatus;
import io.scalecube.cluster.ICluster;
import io.scalecube.transport.Message;

/**
 * @author Anton Kharenko
 */
public class ServiceClientFactory {

  private final ICluster cluster;
  private final IServiceRegistry serviceRegistry;
  private final ServiceProcessor serviceProcessor;

  public ServiceClientFactory(ICluster cluster, IServiceRegistry serviceRegistry, ServiceProcessor serviceProcessor) {
    this.cluster = cluster;
    this.serviceRegistry = serviceRegistry;
    this.serviceProcessor = serviceProcessor;
  }

  public <T> T createClient(Class<T> serviceInterface) {

    final ServiceDefinition serviceDefinition = serviceProcessor.introspectServiceInterface(serviceInterface);

    return Reflection.newProxy(serviceInterface, new InvocationHandler() {
      @Override
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Collection<ServiceReference> serviceReferences = serviceRegistry.serviceLookup(serviceDefinition.serviceName());
        if (serviceReferences.size() == 0) {
          return Futures.immediateFailedFuture(new IllegalStateException("No such service registered"));
        }

        // Router: Choose first reachable member
        ServiceInstance serviceInstance = null;
        for (ServiceReference serviceReference : serviceReferences) {
          ClusterMember member = cluster.membership().member(serviceReference.memberId());
          if (member.status() == ClusterMemberStatus.TRUSTED) {
            serviceInstance = serviceRegistry.serviceInstance(serviceReference);
            break;
          }
        }

        if (serviceInstance == null) {
          return Futures.immediateFailedFuture(new IllegalStateException("No reachable member with such service"));
        }

        // TODO: check args
        // TODO: check method name exists
        String methodName = serviceDefinition.methodName(method);
        return serviceInstance.invoke(methodName, (Message) args[0]);
      }
    });
  }

}
