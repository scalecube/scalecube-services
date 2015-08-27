package io.servicefabric.cluster.gossip;

import io.servicefabric.cluster.Cluster;
import io.servicefabric.cluster.ICluster;
import io.servicefabric.common.Greetings;

/**
 * Basic example for member gossiping between cluster members
 * to run the example Start ClusterNodeA and cluster ClusterNodeB
 * A listen on gossip
 * B spread gossip  
 * @author ronen hamias
 *
 */
public class ClusterNodeB {

	public static void main(String[] args) {
				
		// start cluster node that listen on port 3001 and point to node A as seed node
		ICluster clusterB =  Cluster.newInstance(3001,"localhost:3000").join();
		
		// spread gossip
		clusterB.gossip().spread("hello/world", new Greetings("Greetings from ClusterMember B"));
	}

}
