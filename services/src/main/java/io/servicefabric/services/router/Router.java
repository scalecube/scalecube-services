package io.servicefabric.services.router;

import io.servicefabric.cluster.ClusterMember;
import io.servicefabric.cluster.ClusterMemberStatus;
import io.servicefabric.cluster.ClusterMessage;
import io.servicefabric.cluster.ICluster;
import io.servicefabric.services.registry.ServiceInstance;
import io.servicefabric.services.registry.ServiceReference;
import io.servicefabric.services.registry.ServiceRegistry;
import io.servicefabric.transport.protocol.Message;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;

import rx.functions.Action1;
import rx.functions.Func1;

// dummy router  that just select the first
public class Router {

	private final ICluster cluster;
	private final ServiceRegistry serviceRegistry;



	public Router(ICluster cluster, ServiceRegistry serviceRegistry) {
		this.cluster = cluster;
		this.serviceRegistry = serviceRegistry;
	}

	public ServiceReference select(Collection<ServiceReference> references){
		for(ServiceReference sr : references)
			return sr;

		return null;
	}

	public void route(Message message) throws InvocationTargetException, IllegalAccessException {
		String serviceName = message.header("serviceName");

		Collection<ServiceReference> serviceReferences = serviceRegistry.serviceLookup(serviceName);

		ServiceReference sr = select(serviceReferences);

		if(isLocalService(sr)){
			invokeServiceMethod(message);
		}else{
			routeRemote(message, sr);
		}
	}

	public static final Func1<ClusterMessage, Boolean> MESSAGE_PREDICATE = new Func1<ClusterMessage, Boolean>() {
		@Override
		public Boolean call(ClusterMessage t1) {
			return t1.message().data() != null && t1.message().header("serviceName")!=null;
		}
	};

	public void listen(){
		cluster.listen().filter(MESSAGE_PREDICATE).subscribe(new Action1<ClusterMessage>() {
			@Override
			public void call(ClusterMessage t1) {
				try {
					Router.this.route((Message)t1.message());
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
	}

	private void routeRemote(Message message, ServiceReference sr) {
		ClusterMember member = cluster.membership().member(sr.memberId());

		if(member.status().equals(ClusterMemberStatus.TRUSTED))
			cluster.send(member, message);
	}

	private void invokeServiceMethod(Message message) throws InvocationTargetException, IllegalAccessException {
		ServiceInstance instance = serviceRegistry.getServiceInstance(message.header("serviceName"));
		instance.invoke(message.header("methodName"), message.data());
	}

	private boolean isLocalService(ServiceReference sr) {
		return cluster.membership().localMember().id().equals(sr.memberId()) ;
	} 
}