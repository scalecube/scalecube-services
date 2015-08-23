package servicefabric.cluster.gossip;

import io.servicefabric.cluster.Cluster;
import io.servicefabric.cluster.ICluster;

public class ClusterNodeB {

	public static void main(String[] args) {
		
		// start cluster node that listen on port 3001 and point to node A as seed node
		ICluster clusterB =  Cluster.newInstance(3001,"localhost:3000").join();
		
		// spread gossip
		clusterB.gossip().spread("hello/world",null);
	}

}
