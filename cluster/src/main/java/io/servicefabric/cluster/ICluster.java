package io.servicefabric.cluster;

import com.google.common.util.concurrent.ListenableFuture;
import io.servicefabric.cluster.gossip.IGossipProtocol;
import io.servicefabric.transport.ITransportChannel;
import io.servicefabric.transport.TransportMessage;
import rx.Observable;

/**
 * @author Anton Kharenko
 */
public interface ICluster {

	ITransportChannel to(ClusterMember member);

	Observable<TransportMessage> listen();

	IGossipProtocol gossip();

	IClusterMembership membership();

	ICluster join();

	ListenableFuture<Void> leave();

}
