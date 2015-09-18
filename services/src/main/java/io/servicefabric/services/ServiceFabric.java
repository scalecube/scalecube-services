package io.servicefabric.services;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import io.servicefabric.cluster.Cluster;
import io.servicefabric.cluster.ClusterMember;
import io.servicefabric.cluster.ICluster;
import io.servicefabric.services.registry.ServiceInstance;
import io.servicefabric.services.registry.ServiceReference;
import io.servicefabric.services.registry.ServiceRegistry;
import io.servicefabric.services.router.Router;
import io.servicefabric.transport.protocol.Message;

public class ServiceFabric {


	final ServiceRegistry serviceRegistry;

	final ICluster cluster;

	final Router router ;

	public ServiceFabric(int port){
		cluster = Cluster.newInstance(port);
		serviceRegistry  = new ServiceRegistry(cluster);
		router = new Router(cluster,serviceRegistry);
	}

	public void registerService(Object serviceObject){
		serviceRegistry.registerService(serviceObject);
	}

	public void start() {
		cluster.join();
		serviceRegistry.start();
	}

	public void call(Message message) throws InvocationTargetException, IllegalAccessException{
		String serviceName = message.header("serviceName");
		Collection<ServiceReference> serviceReferences = serviceRegistry.serviceLookup(serviceName);
		router.route(message,serviceReferences);
	}

}
