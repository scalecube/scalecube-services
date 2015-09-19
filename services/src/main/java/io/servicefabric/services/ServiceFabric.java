package io.servicefabric.services;

import io.servicefabric.cluster.Cluster;
import io.servicefabric.cluster.ICluster;
import io.servicefabric.services.registry.ServiceRegistry;
import io.servicefabric.services.router.Router;
import io.servicefabric.transport.protocol.Message;

import java.lang.reflect.InvocationTargetException;

public class ServiceFabric {


	final ServiceRegistry serviceRegistry;

	final ICluster cluster;

	final Router router ;

	public ServiceFabric(int port){
		cluster = Cluster.newInstance(port);
		serviceRegistry  = new ServiceRegistry(cluster);
		router = new Router(cluster,serviceRegistry);
	}

	public ServiceFabric(int port, String seeds) {
		cluster = Cluster.newInstance(port,seeds);
		serviceRegistry  = new ServiceRegistry(cluster);
		router = new Router(cluster,serviceRegistry);
	}

	public void registerService(Object serviceObject){
		serviceRegistry.registerService(serviceObject);
	}

	public void start() {
		cluster.join();
		serviceRegistry.start();
		router.listen();
	}

	public void invoke(Message message) throws InvocationTargetException, IllegalAccessException, ServiceNotfoundException{	
		router.route(message);
	}

}
