package servicefabric.cluster.gossip;

import io.servicefabric.cluster.Cluster;
import io.servicefabric.cluster.ICluster;
import io.servicefabric.transport.TransportTypeRegistry;
import servicefabric.common.Greetings;

public class ClusterNodeB {

	
	public static void main(String[] args) {
		// Register data types
		TransportTypeRegistry.getInstance().registerType("hello/world", Greetings.class);
		
		// start cluster node that listen on port 3001 and point to node A as seed node
		ICluster clusterB =  Cluster.newInstance(3001,"localhost:3000").join();
		
		// spread gossip
		clusterB.gossip().spread("hello/world", new Greetings("Greetings from ClusterMember B"));
	}

}
