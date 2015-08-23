package servicefabric.cluster.transport;

import com.google.common.util.concurrent.SettableFuture;

import io.servicefabric.cluster.Cluster;
import io.servicefabric.cluster.ICluster;
import io.servicefabric.transport.TransportEndpoint;
import io.servicefabric.transport.protocol.Message;

public class ClusterNodeB {

	public static void main(String[] args) {
		
		// start cluster node that listen on port 3001 and point to node A as seed node
		ICluster clusterB =  Cluster.newInstance(3001,"localhost:3000").join();
		
		// send transport message to ClusterMemeberA (tcp://A@localhost:3000)
		TransportEndpoint endpoint = TransportEndpoint.from("tcp://A@localhost:3000");
		SettableFuture<Void> promis = SettableFuture.create();
		clusterB.transport().to(endpoint).send(new Message("hello/world"),promis);
		
	}

}
