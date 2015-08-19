package io.servicefabric.cluster.examples;

import io.servicefabric.cluster.ClusterBuilder;
import io.servicefabric.cluster.ClusterEndpoint;
import io.servicefabric.cluster.ICluster;
import io.servicefabric.transport.TransportEndpoint;

/**
 * @author Anton Kharenko
 */
public class ClusterAPIPlayground {

	public static void main(String[] args) throws InterruptedException {
		ICluster cb1 = ClusterBuilder.newInstance(4001, "localhost:4001,localhost:4002").build();
		cb1.start();

		ICluster cb2 = ClusterBuilder.newInstance(4002, "localhost:4001,localhost:4002").build();
		cb2.start();

		ICluster cb3 = ClusterBuilder.newInstance(4003, "localhost:4001,localhost:4002").build();
		cb3.start();

		ICluster cb4 = ClusterBuilder.newInstance(4004, "localhost:4001,localhost:4002").build();
		cb4.start();
	}

}
