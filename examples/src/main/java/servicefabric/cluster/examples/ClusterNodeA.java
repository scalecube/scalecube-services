package servicefabric.cluster.examples;

import io.servicefabric.cluster.Cluster;
import io.servicefabric.cluster.ICluster;
import io.servicefabric.cluster.gossip.Gossip;

import java.util.concurrent.atomic.AtomicInteger;

import rx.functions.Action1;

public class ClusterNodeA {

	public static void main(String[] args) {
	
		ICluster clusterA = Cluster.newInstance(0).join();
		
		System.out.println(clusterA.members()); 
		final AtomicInteger ai = new AtomicInteger(0); 
		clusterA.gossip().listen().subscribe(new Action1<Gossip>() {
			@Override
			public void call(Gossip g) {
				System.out.println(g);
			}
		});
	}
	
}
