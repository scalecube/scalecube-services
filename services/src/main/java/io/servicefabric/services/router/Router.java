package io.servicefabric.services.router;

import io.servicefabric.cluster.ClusterMember;
import io.servicefabric.cluster.ICluster;
import io.servicefabric.services.registry.ServiceInstance;
import io.servicefabric.services.registry.ServiceReference;
import io.servicefabric.services.registry.ServiceRegistry;
import io.servicefabric.transport.protocol.Message;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;

// dummy router  that just select the first
public class Router {

	private ICluster cluster;
	private ServiceRegistry serviceRegistry;

	public Router(ICluster cluster, ServiceRegistry serviceRegistry) {
		this.cluster = cluster;
		this.serviceRegistry = serviceRegistry;
	}

	public ServiceReference select(Collection<ServiceReference> references){
		for(ServiceReference sr : references)
			return sr;
		return null;
	}

	public void route(Message message, Collection<ServiceReference> serviceReferences) throws InvocationTargetException, IllegalAccessException {
		ServiceReference sr = select(serviceReferences);
		if(isLocalService(sr)){
			routeLocal(message);
		}else{
			routeRemote(message, sr);
		}
	}
	
	private void routeRemote(Message message, ServiceReference sr) {
		ClusterMember member = cluster.membership().member(sr.memberId());
		cluster.send(member, message);
	}

	private void routeLocal(Message message)
			throws InvocationTargetException, IllegalAccessException {
		ServiceInstance instance = serviceRegistry.getServiceInstance(message.header("serviceName"));
		instance.invoke(message.header("methodName"), message.data());
	}

	private boolean isLocalService(ServiceReference sr) {
		return sr.memberId().equals(cluster.membership().localMember().id());
	} 
}