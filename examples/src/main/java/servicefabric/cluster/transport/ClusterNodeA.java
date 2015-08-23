package servicefabric.cluster.transport;

import io.servicefabric.cluster.Cluster;
import io.servicefabric.cluster.ICluster;
import io.servicefabric.transport.TransportMessage;
import rx.functions.Action1;
import rx.functions.Func1;

public class ClusterNodeA {

	public static void main(String[] args) {
	
		// start cluster node that listen on port 3000
		ICluster clusterA = Cluster.newInstance(3000).join();
		
		// Filter and subscribe to hello/world message:
		clusterA.transport().listen().filter(new Func1<TransportMessage, Boolean>() {
			@Override
			public Boolean call(TransportMessage t1) {
				return t1.message().qualifier().equals("hello/world");
			}
		}).subscribe(new Action1<TransportMessage>() {
			@Override
			public void call(TransportMessage t1) {
				System.out.println(t1.message());
			}
		});
	}
	
}
