package io.scalecube.services;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.reflect.Reflection;
import com.google.common.util.concurrent.Futures;

import io.scalecube.cluster.ICluster;
import io.scalecube.cluster.Member;
import io.scalecube.transport.Message;

public class ServiceClientFactory {

  private final ICluster cluster;
  private final IServiceRegistry serviceRegistry;
  private final ServiceProcessor serviceProcessor;
  private ConcurrentMap<String, ServiceDefinition> serviceDefinitions;

  public ServiceClientFactory(ICluster cluster, IServiceRegistry serviceRegistry, ServiceProcessor serviceProcessor) {
    this.cluster = cluster;
    this.serviceRegistry = serviceRegistry;
    this.serviceProcessor = serviceProcessor;
  }

  public <T> T createClient(Class<T> serviceInterface) {
    this.serviceDefinitions = serviceProcessor.introspectServiceInterface(serviceInterface);

    return Reflection.newProxy(serviceInterface, new InvocationHandler() {

      @Override
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        try {
          
            ServiceInstance  serviceInstance = findInstance(method);
            
            if(serviceInstance!=null){
              return serviceInstance.invoke(method.getName(), (Message) args[0]);
            }else{
              return Futures
                  .immediateFailedFuture(new IllegalStateException("No reachable member with such service"));
            }
        } catch (RuntimeException e){
          return Futures
              .immediateFailedFuture(new IllegalStateException("No reachable member with such service"));
        }
      }
      
      private ServiceInstance findInstance(Method method) {
        ServiceDefinition serviceDefinition = serviceDefinitions.get(method.getName());
        Collection<ServiceReference> serviceReferences = serviceRegistry.serviceLookup(serviceDefinition.serviceName());
        
        if(serviceReferences.isEmpty()){
         ServiceInstance instance = serviceRegistry.findRemoteInstance(serviceDefinition.serviceName());
         return  instance;
        }
        
        if (serviceReferences.size() == 0) {
          throw new IllegalStateException("No such service registered");
        }
        else{
          return selectServiceInstance(serviceReferences);
        }
      }
    });
  }

  // Router: Choose first reachable member
  // TODO: maybe choose more sofisticated selection
  private ServiceInstance selectServiceInstance(Collection<ServiceReference> serviceReferences) {
    List<ServiceInstance> instances = new ArrayList<ServiceInstance>();
    for (ServiceReference serviceReference : serviceReferences) {
      Member member = cluster.member(serviceReference.memberId());
      if (member != null) {
        instances.add(serviceRegistry.serviceInstance(serviceReference));
      }
    }
    
    int selection = randomSelection(instances.size());
    return instances.get(selection);
  }

  private int randomSelection(int size) {
    if (size >= 0) {
      return ThreadLocalRandom.current().nextInt(size);
    } else {
      return 0;
    }
  }
}
