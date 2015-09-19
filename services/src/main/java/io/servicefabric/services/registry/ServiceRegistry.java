package io.servicefabric.services.registry;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.functions.Action1;
import rx.functions.Func1;

import java.awt.geom.CubicCurve2D;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.servicefabric.cluster.ClusterMember;
import io.servicefabric.cluster.ClusterMessage;
import io.servicefabric.cluster.ICluster;
import io.servicefabric.services.MultimapCache;
import io.servicefabric.services.annotations.ServiceAnnotationsProcessor;
import io.servicefabric.transport.protocol.Message;
import io.servicefabric.transport.protocol.MessageHeaders;

public class ServiceRegistry implements IServiceRegistry {

	private static final Logger LOGGER = LoggerFactory.getLogger(ServiceRegistry.class);

	private MultimapCache<String, ServiceReference> serviceRegistryCache = new MultimapCache<>();

	private final ConcurrentMap<String, ServiceInstance> localServices = new ConcurrentHashMap();

	private final ICluster cluster;

	public ServiceRegistry(ICluster cluster) {
		this.cluster = cluster;
	}

	String localMemberId;

	ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
	public void start() {

		localMemberId = cluster.membership().localMember().id();

		listen();

		for(ServiceInstance serviceInstance : localServices.values()){
			ServiceReference serviceReference  = new ServiceReference(serviceInstance.getServiceName() , localMemberId);	 
			serviceRegistryCache.put(serviceInstance.getServiceName(), serviceReference);
			//send gossip about service registration
			cluster.gossip().spread(new Message(serviceReference));
		}

		// TODO : do I need to clean registration of services for removed members or just clean up them periodically?
		// TODO : listen for gossips of service registration

		// TODO : send sync events
		scheduler.scheduleAtFixedRate(new Runnable(){
			@Override
			public void run() {
				sync();
				
			}}, 10, 10, TimeUnit.SECONDS);
	}

	public static final Func1<Message, Boolean> SERVICE_REFERENCE_PREDICATE = new Func1<Message, Boolean>() {
		@Override
		public Boolean call(Message message) {
			return message.data() instanceof ServiceReference;
		}
	};

	private void listen(){
		cluster.gossip().listen().filter(SERVICE_REFERENCE_PREDICATE).subscribe(new Action1<Message>() {
			@Override
			public void call(Message gossip) {

				ServiceReference ref = (ServiceReference) gossip.data();
				if(!ref.memberId().equals(localMemberId)){
					serviceRegistryCache.put(ref.name(), ref);
				}
			}
		});
	}
	private void sync(){
		Collection<Collection<ServiceReference>> references = serviceRegistryCache.values();

		for(Collection<ServiceReference> collection : references){
			for(ServiceReference ref : collection){
				cluster.gossip().spread(new Message(ref));
			}
		}

	}
	public void registerService(Object serviceObject) {
		Collection<ServiceInstance> serviceInstances = ServiceAnnotationsProcessor.processService(serviceObject);
		for (ServiceInstance serviceInstance : serviceInstances) {
			registerService(serviceInstance);
		}
	}

	public void registerService(ServiceInstance serviceInstance) {
		localServices.put(serviceInstance.getServiceName(), serviceInstance);
	}

	@Override
	public Collection<ServiceReference> serviceLookup(final String serviceName) {
		checkArgument(serviceName != null, "Service name can't be null");
		Collection<ServiceReference> serviceReferences;
		serviceReferences = serviceRegistryCache.get(serviceName);
		return serviceReferences == null ? Collections.<ServiceReference>emptySet() : serviceReferences;
	}

	public ServiceInstance getServiceInstance(String serviceName){
		return localServices.get(serviceName);
	}
}
