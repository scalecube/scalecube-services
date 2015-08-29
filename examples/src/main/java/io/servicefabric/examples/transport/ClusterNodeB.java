package io.servicefabric.examples.transport;

import com.google.common.util.concurrent.SettableFuture;

import io.servicefabric.cluster.Cluster;
import io.servicefabric.cluster.ClusterMember;
import io.servicefabric.cluster.ICluster;
import io.servicefabric.examples.Greetings;
import io.servicefabric.transport.protocol.Message;

import java.util.List;

/**
 * Basic example for member transport between cluster members
 * to run the example Start ClusterNodeA and cluster ClusterNodeB
 * A listen on transport messages
 * B send message to member A 
 * @author ronen hamias
 *
 */
public class ClusterNodeB {

	public static void main(String[] args) throws InterruptedException {
		// Start cluster node that listen on port 3001 and point to node A as seed node
		ICluster clusterB =  Cluster.newInstance(3001, "localhost:3000").join();

		// Wait for some time until members are synchronized
		Thread.sleep(100);

		// Send greeting message to other cluster members
		List<ClusterMember> members = clusterB.membership().members();

		for (ClusterMember member : members) {
			if (!clusterB.membership().isLocalMember(member)) {
				SettableFuture<Void> promise = SettableFuture.create();
				clusterB.to(member).send(new Message(new Greetings("Greetings from ClusterMember B")), promise);
			}
		}
	}

}
