package servicefabric.cluster.examples;

import io.servicefabric.cluster.Cluster;
import io.servicefabric.cluster.ICluster;
import io.servicefabric.transport.TransportEndpoint;
import io.servicefabric.transport.protocol.Message;

import com.google.common.util.concurrent.SettableFuture;

public class ClusterNodeB {

	public static void main(String[] args) {
		
		ICluster clusterB =  Cluster.newInstance(3001,"localhost:3000").join();
			
		TransportEndpoint te = TransportEndpoint.from("tcp://A@localhost:3000");
		
		SettableFuture<Void> promise = SettableFuture.create();
		
		clusterB.transport().to(te).send(new Message("hello/world"), promise );
		
		clusterB.gossip().spread("hello/world",null);
	}

}
