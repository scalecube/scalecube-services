package servicefabric.cluster.gossip;

import io.servicefabric.cluster.Cluster;
import io.servicefabric.cluster.ICluster;
import io.servicefabric.cluster.gossip.Gossip;
import io.servicefabric.transport.TransportTypeRegistry;
import rx.functions.Action1;
import servicefabric.common.Greetings;

public class ClusterNodeA {


	public static void main(String[] args) {
		// Register data types
		TransportTypeRegistry.getInstance().registerType("hello/world", Greetings.class);

		// start cluster node that listen on port 3000
		ICluster clusterA = Cluster.newInstance(3000).join();

		// subscribe to all gossip messages:
		clusterA.gossip().listen().subscribe(new Action1<Gossip>() {
			@Override
			public void call(Gossip gossip) {
				// print out the gossip message
				System.out.println("Gossip message:"  + gossip.getData());
			}
		});
	}

}
